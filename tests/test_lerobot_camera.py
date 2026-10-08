"""Tests for _lerobot/camera.py."""

import json

import pytest
from lerobot_fakes import make_session, tar_page

from cognitive3dpy._client import C3DNotFoundError, get_base_url
from cognitive3dpy._lerobot.camera import (
    CameraDownloader,
    CameraFrameCache,
    StreamUnavailable,
    read_stream_page,
)
from cognitive3dpy._lerobot.http import HttpClient
from cognitive3dpy._lerobot.sessions import fetch_session

FRAME = "front/0000000000100_0000000000"


def frames_of(page: bytes):
    got = []
    chunks = iter([page[i : i + 700] for i in range(0, len(page), 700)])
    cursor = read_stream_page(chunks, lambda d, s, b: got.append((d, s, b)))
    return cursor, got


def test_read_stream_page_complete():
    cursor_doc = {"frames": 1, "next": None, "complete": True}
    page = tar_page(
        [
            (f"{FRAME}.jpg", b"a" * 10),
            (f"{FRAME}.json", b"{}"),
            ("_cursor.json", json.dumps(cursor_doc).encode()),
        ]
    )
    cursor, got = frames_of(page)
    assert cursor == cursor_doc
    assert got == [(100, 0, b"a" * 10)]


def test_read_stream_page_truncated_mid_member():
    page = tar_page([(f"{FRAME}.jpg", b"a" * 2000), ("_cursor.json", b"{}")])
    cursor, _ = frames_of(page[:1200])
    assert cursor is None


def test_read_stream_page_without_cursor_is_truncated():
    cursor, got = frames_of(tar_page([(f"{FRAME}.jpg", b"a" * 10)]))
    assert cursor is None and len(got) == 1


@pytest.fixture
def setup(edge, tmp_path):
    fake = edge.add(make_session())
    http = HttpClient("test-api-key", get_base_url(), sleep=lambda _s: None)
    session = fetch_session(
        http, fake.project_id, fake.session_id, ["camera"], progress=False
    )
    cache = CameraFrameCache(tmp_path / "frames", http.host, 1234, fake.session_id)

    def download(**kwargs):
        downloader = CameraDownloader(http, progress=False, **kwargs)
        return downloader.download(cache, 1234, fake.session_id, session.cameras)

    yield fake, session, cache, download
    http.close()


def assert_all_cached(fake, session, cache):
    for ref in session.cameras["front"]:
        key = (ref.version_id, ref.camera_id, ref.date, ref.seq)
        assert cache.read(ref) == fake.frames[key]


def test_stream_transport_paged(setup, edge):
    fake, session, cache, download = setup
    stats = download(page_limit=6)
    assert_all_cached(fake, session, cache)
    assert stats.transports == {"stream"} and stats.via_stream == 20
    assert edge.calls["stream"] == 4 and edge.calls["frame"] == 0


def test_falls_back_to_per_frame_when_the_stream_route_is_missing(setup, edge):
    fake, session, cache, download = setup
    edge.stream_available = False
    stats = download()
    assert_all_cached(fake, session, cache)
    assert stats.transports == {"per-frame"} and stats.via_per_frame == 20
    assert edge.calls["stream"] == 1  # probed once, then remembered


def test_stream_only_transport_raises_when_the_route_is_missing(setup, edge):
    edge.stream_available = False
    with pytest.raises(StreamUnavailable):
        setup[3](transport="stream")


def test_truncated_pages_resume_from_the_last_full_frame(setup, edge):
    fake, session, cache, download = setup
    edge.truncate_streams = 2
    stats = download(page_limit=8)
    assert_all_cached(fake, session, cache)
    assert stats.via_stream == 20 and stats.via_per_frame == 0
    assert edge.calls["stream"] > 3


def test_cached_frames_are_not_downloaded_again(setup, edge):
    download = setup[3]
    download()
    calls = dict(edge.calls)
    stats = download()
    assert stats.from_cache == 20 and stats.transports == {"cache"}
    assert dict(edge.calls) == calls


def test_resume_streams_only_the_missing_tail(setup):
    fake, session, cache, download = setup
    for ref in session.cameras["front"][:12]:
        key = (ref.version_id, ref.camera_id, ref.date, ref.seq)
        cache.write(ref, fake.frames[key])
    stats = download()
    assert stats.from_cache == 12 and stats.via_stream == 8
    assert_all_cached(fake, session, cache)


def test_a_frame_the_api_cannot_serve_raises(setup, edge):
    fake, session, _, download = setup
    edge.stream_available = False
    victim = session.cameras["front"][3]
    del fake.frames[(victim.version_id, victim.camera_id, victim.date, victim.seq)]
    with pytest.raises(C3DNotFoundError):
        download()


def test_an_auth_outage_mid_download_is_ridden_out(setup, edge):
    fake, session, cache, download = setup
    edge.auth_outage = 3
    download(transport="per-frame", concurrency=1)
    assert_all_cached(fake, session, cache)


def test_unknown_transport():
    with pytest.raises(ValueError, match="camera_transport"):
        CameraDownloader(None, transport="carrier-pigeon")
