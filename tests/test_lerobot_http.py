"""Tests for _lerobot/http.py."""

import httpx
import pytest
import respx

from cognitive3dpy._client import C3DAPIError, C3DAuthError, C3DNotFoundError
from cognitive3dpy._lerobot.errors import C3DRetentionError
from cognitive3dpy._lerobot.http import HttpClient, redact_key

KEY = "orgkey-VerySecretKeyValue4321"
BASE = "https://fake.test"
URL = f"{BASE}/v0/a"


def make_client(**kwargs):
    sleeps = []
    client = HttpClient(KEY, BASE, sleep=sleeps.append, **kwargs)
    return client, sleeps


@respx.mock
def test_sends_key_user_agent_and_absolute_path():
    route = respx.get(f"{BASE}/v0/projects/1/thing").mock(
        return_value=httpx.Response(200, content=b"ok")
    )
    client, _ = make_client()
    assert client.request_bytes("GET", "/v0/projects/1/thing", context="t") == b"ok"
    request = route.calls[0].request
    assert request.headers["Authorization"] == KEY
    assert request.headers["User-Agent"].startswith("cognitive3dpy/")


@respx.mock
def test_retries_5xx_and_honours_retry_after():
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(503, headers={"Retry-After": "7"}),
            httpx.Response(502),
            httpx.Response(200, content=b"x"),
        ]
    )
    client, sleeps = make_client()
    assert client.request_bytes("GET", "/v0/a", context="t") == b"x"
    assert route.call_count == 3
    assert sleeps[0] == 7.0 and 0 < sleeps[1] <= 1.0


@respx.mock
def test_gives_up_after_max_retries():
    route = respx.get(URL).mock(return_value=httpx.Response(503))
    client, _ = make_client(max_retries=2)
    with pytest.raises(C3DAPIError) as e:
        client.request_bytes("GET", "/v0/a", context="ctx")
    assert e.value.status == 503 and route.call_count == 3


@respx.mock
def test_transport_errors_are_retried_then_raised():
    respx.get(URL).mock(
        side_effect=[httpx.ConnectError("boom"), httpx.Response(200, content=b"ok")]
    )
    client, _ = make_client()
    assert client.request_bytes("GET", "/v0/a", context="t") == b"ok"

    respx.get(URL).mock(side_effect=httpx.ReadTimeout("slow"))
    client, _ = make_client(max_retries=2)
    with pytest.raises(C3DAPIError, match="ReadTimeout"):
        client.request_bytes("GET", "/v0/a", context="t")


@respx.mock
def test_first_401_fails_fast_and_never_leaks_the_key():
    route = respx.get(URL).mock(return_value=httpx.Response(401))
    client, sleeps = make_client()
    with pytest.raises(C3DAuthError) as e:
        client.request_bytes("GET", "/v0/a", context="ctx")
    assert route.call_count == 1 and not sleeps
    assert "VerySecret" not in str(e.value) and "orgkey-…4321" in str(e.value)


@respx.mock
def test_401_after_the_key_worked_is_retried():
    """The API answers 401 when its auth lookup fails; a working key gets time."""
    respx.get(URL).mock(
        side_effect=[
            httpx.Response(200, content=b"1"),
            httpx.Response(401),
            httpx.Response(401),
            httpx.Response(200, content=b"2"),
        ]
    )
    client, sleeps = make_client()
    assert client.request_bytes("GET", "/v0/a", context="t") == b"1"
    assert client.request_bytes("GET", "/v0/a", context="t") == b"2"
    assert sleeps == [5.0, 10.0]


@respx.mock
def test_401_after_success_eventually_gives_up():
    respx.get(URL).mock(side_effect=[httpx.Response(200)] + [httpx.Response(401)] * 4)
    client, sleeps = make_client(max_auth_retries=3)
    client.request_bytes("GET", "/v0/a", context="t")
    with pytest.raises(C3DAuthError):
        client.request_bytes("GET", "/v0/a", context="t")
    assert len(sleeps) == 3


@pytest.mark.parametrize(
    "status,error",
    [(404, C3DNotFoundError), (410, C3DRetentionError), (400, C3DAPIError)],
)
@respx.mock
def test_status_mapping(status, error):
    route = respx.get(URL).mock(return_value=httpx.Response(status, text="nope"))
    client, _ = make_client()
    with pytest.raises(error) as e:
        client.request_bytes("GET", "/v0/a", context="ctx")
    assert e.value.status == status and e.value.body == "nope"
    assert route.call_count == 1


@respx.mock
def test_open_stream_retries_the_open_and_yields_the_body():
    route = respx.get(URL).mock(
        side_effect=[httpx.Response(503), httpx.Response(200, content=b"streamed")]
    )
    client, _ = make_client()
    with client.open_stream("GET", "/v0/a", context="t") as response:
        assert b"".join(response.iter_bytes()) == b"streamed"
    assert route.call_count == 2


@respx.mock
def test_progress_bar_reads_the_full_body():
    body = b"z" * 5000
    respx.get(URL).mock(
        return_value=httpx.Response(
            200, content=body, headers={"Content-Length": "5000"}
        )
    )
    client, _ = make_client()
    assert client.request_bytes("GET", "/v0/a", context="t", progress_desc="f") == body


def test_redact_key_and_key_types():
    assert redact_key("projectkey-abcdefgh9876") == "projectkey-…9876"
    assert redact_key("plainsecret1234") == "…1234"
    client, _ = make_client()
    assert client.key_type == "organization" and client.host == "fake.test"
    assert HttpClient("projectkey-x", BASE).key_type == "project"
