"""Tests for sessions.py."""

import logging

import httpx
import pytest
import respx
from helpers import load_fixture

from cognitive3dpy._client import BASE_URL
from cognitive3dpy.sessions import c3d_sessions

SESSIONS_URL = f"{BASE_URL}/v0/datasets/sessions/paginatedListQueries"
PROJECT_URL = f"{BASE_URL}/v0/projects/1234"


@respx.mock
def test_sessions_project_mode_returns_dataframe():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_page.json"))
    )
    df = c3d_sessions(start_date="2025-01-01", end_date="2026-01-01")
    assert df.shape[0] == 2
    assert "session_id" in df.columns


@respx.mock
def test_sessions_compact_true_limits_columns():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_page.json"))
    )
    df = c3d_sessions(start_date="2025-01-01", end_date="2026-01-01", compact=True)
    assert "session_id" in df.columns
    # Raw API fields like 'sessionId' should not appear
    assert "sessionId" not in df.columns


@respx.mock
def test_sessions_compact_false_returns_more_columns():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_page.json"))
    )
    compact_df = c3d_sessions(
        start_date="2025-01-01", end_date="2026-01-01", compact=True
    )
    full_df = c3d_sessions(
        start_date="2025-01-01", end_date="2026-01-01", compact=False
    )
    assert full_df.shape[1] >= compact_df.shape[1]


@respx.mock
def test_sessions_duration_converted_to_seconds():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_page.json"))
    )
    df = c3d_sessions(start_date="2025-01-01", end_date="2026-01-01")
    assert "duration_s" in df.columns
    assert df["duration_s"][0] == 1800.0


@respx.mock
def test_sessions_empty_returns_empty_dataframe():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json={"count": 0, "pages": 0, "results": []})
    )
    df = c3d_sessions(start_date="2025-01-01", end_date="2026-01-01", warn_empty=False)
    assert df.is_empty()


@respx.mock
def test_sessions_scene_mode_joins_scene_name():
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_page.json"))
    )
    df = c3d_sessions(
        start_date="2025-01-01",
        end_date="2026-01-01",
        session_type="scene",
        compact=False,
    )
    assert "scene_name" in df.columns


@respx.mock
def test_sessions_output_pandas():
    pytest.importorskip("pyarrow")
    pd = __import__("pandas")
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_page.json"))
    )
    df = c3d_sessions(start_date="2025-01-01", end_date="2026-01-01", output="pandas")
    assert isinstance(df, pd.DataFrame)


def test_sessions_rejects_invalid_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_sessions(project_id="1234")


def test_sessions_rejects_float_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_sessions(project_id=1234.0)


def test_sessions_rejects_invalid_scene_id():
    with pytest.raises(TypeError, match="scene_id must be a str"):
        c3d_sessions(scene_id=12345)


def test_sessions_rejects_invalid_scene_version_id():
    with pytest.raises(TypeError, match="scene_version_id must be an int"):
        c3d_sessions(scene_version_id="1")


def test_sessions_rejects_bool_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_sessions(project_id=True)


def test_sessions_rejects_zero_n():
    with pytest.raises(ValueError, match="n must be a positive integer"):
        c3d_sessions(n=0)


def test_sessions_rejects_negative_n():
    with pytest.raises(ValueError, match="n must be a positive integer"):
        c3d_sessions(n=-1)


def test_sessions_rejects_float_n():
    with pytest.raises(ValueError, match="n must be a positive integer"):
        c3d_sessions(n=10.5)


def test_sessions_rejects_bool_n():
    with pytest.raises(ValueError, match="n must be a positive integer"):
        c3d_sessions(n=True)


def test_sessions_rejects_bool_scene_version_id():
    with pytest.raises(TypeError, match="scene_version_id must be an int"):
        c3d_sessions(scene_version_id=True)


@respx.mock
def test_sessions_progress_logged(caplog):
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_page.json"))
    )
    with caplog.at_level(logging.INFO, logger="cognitive3dpy._pagination"):
        c3d_sessions(start_date="2025-01-01", end_date="2026-01-01")
    assert any("Fetching sessions" in m for m in caplog.messages)


@respx.mock
def test_sessions_no_stdout_output(capsys):
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_page.json"))
    )
    c3d_sessions(start_date="2025-01-01", end_date="2026-01-01")
    assert capsys.readouterr().out == ""
