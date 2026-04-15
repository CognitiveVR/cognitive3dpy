"""Tests for events.py."""

import logging

import httpx
import pytest
import respx
from helpers import load_fixture

from cognitive3dpy._client import BASE_URL
from cognitive3dpy.events import _unnest_events, c3d_events

SESSIONS_URL = f"{BASE_URL}/v0/datasets/sessions/paginatedListQueries"
OBJECTS_URL = f"{BASE_URL}/v0/projects/1234/objects"
PROJECT_URL = f"{BASE_URL}/v0/projects/1234"


@respx.mock
def test_events_one_row_per_event():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_with_events.json"))
    )
    respx.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    df = c3d_events(start_date="2025-01-01", end_date="2026-01-01")
    # fixture has 1 session with 2 events
    assert df.shape[0] == 2


@respx.mock
def test_events_expected_columns():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_with_events.json"))
    )
    respx.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    df = c3d_events(start_date="2025-01-01", end_date="2026-01-01")
    for col in (
        "session_id",
        "event_name",
        "event_date",
        "object_id",
        "object",
        "scene_name",
    ):
        assert col in df.columns, f"Missing column: {col}"


@respx.mock
def test_events_object_resolved_to_name():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_with_events.json"))
    )
    respx.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    df = c3d_events(start_date="2025-01-01", end_date="2026-01-01")
    # First event has object "obj-1" → "Fire Extinguisher"
    assert df["object"][0] == "Fire Extinguisher"


@respx.mock
def test_events_prop_prefix_applied():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_with_events.json"))
    )
    respx.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    df = c3d_events(start_date="2025-01-01", end_date="2026-01-01")
    # "intensity" property should be prefixed
    assert "prop_intensity" in df.columns


@respx.mock
def test_events_scene_name_joined():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_with_events.json"))
    )
    respx.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    df = c3d_events(start_date="2025-01-01", end_date="2026-01-01")
    # scene_version_id=11 → "Training Room"
    assert df["scene_name"][0] == "Training Room"


@respx.mock
def test_events_empty_returns_empty_dataframe():
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json={"count": 0, "pages": 0, "results": []})
    )
    df = c3d_events(start_date="2025-01-01", end_date="2026-01-01", warn_empty=False)
    assert df.is_empty()


@respx.mock
def test_events_truncation_logged(caplog):
    truncated_fixture = {
        "count": 1,
        "pages": 1,
        "results": [
            {
                "sessionId": "session-trunc",
                "projectId": 1234,
                "participantId": "p001",
                "userKey": "user-key-1",
                "deviceId": "device-001",
                "user": "device-001",
                "date": "2025-06-01T10:00:00Z",
                "duration": 1800000,
                "eventsLimited": True,
                "events": [],
            }
        ],
    }
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=truncated_fixture)
    )
    respx.get(OBJECTS_URL).mock(return_value=httpx.Response(200, json=[]))
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    with caplog.at_level(logging.WARNING, logger="cognitive3dpy.events"):
        c3d_events(start_date="2025-01-01", end_date="2026-01-01", warn_empty=False)
    assert any("truncated" in m for m in caplog.messages)


def test_events_rejects_string_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_events(project_id="1234")


def test_events_rejects_float_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_events(project_id=1234.0)


def test_events_rejects_bool_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_events(project_id=True)


def test_events_rejects_zero_n():
    with pytest.raises(ValueError, match="n must be a positive integer"):
        c3d_events(n=0)


def test_events_rejects_negative_n():
    with pytest.raises(ValueError, match="n must be a positive integer"):
        c3d_events(n=-1)


def test_events_rejects_float_n():
    with pytest.raises(ValueError, match="n must be a positive integer"):
        c3d_events(n=10.5)


def test_events_rejects_bool_n():
    with pytest.raises(ValueError, match="n must be a positive integer"):
        c3d_events(n=True)


@respx.mock
def test_events_no_stdout_output(capsys):
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("sessions_with_events.json"))
    )
    respx.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    c3d_events(start_date="2025-01-01", end_date="2026-01-01")
    assert capsys.readouterr().out == ""


# --- Event property collision detection (DS-561) ---


def _make_session(events):
    """Build a minimal session dict with the given events list."""
    return {
        "projectId": 1234,
        "sessionId": "sess-1",
        "participantId": "p1",
        "userKey": "uk1",
        "deviceId": "dev1",
        "date": "2025-06-01T10:00:00Z",
        "duration": 60000,
        "events": events,
    }


def test_unnest_events_prop_collision_keeps_first(recwarn):
    """Two property keys that clean to the same prop_* name."""
    event = {
        "name": "test_event",
        "date": "2025-06-01T10:00:00Z",
        "properties": {"my.prop": "first", "my_prop": "second"},
    }
    df = _unnest_events([_make_session([event])])
    assert "prop_my_prop" in df.columns
    assert df["prop_my_prop"][0] == "first"
    assert any("collide with another property" in str(w.message) for w in recwarn)


def test_unnest_events_prop_collision_does_not_overwrite_base(recwarn):
    """Even without the prop_ prefix making base collisions impossible,
    verify that properties never overwrite standard event fields."""
    event = {
        "name": "test_event",
        "date": "2025-06-01T10:00:00Z",
        "properties": {"intensity": 0.8},
    }
    df = _unnest_events([_make_session([event])])
    # Standard fields remain intact
    assert df["session_id"][0] == "sess-1"
    assert df["event_name"][0] == "test_event"
    # Property is prefixed and separate
    assert df["prop_intensity"][0] == 0.8


def test_unnest_events_no_collision_no_warning(recwarn):
    """Normal properties should produce no warnings."""
    event = {
        "name": "test_event",
        "date": "2025-06-01T10:00:00Z",
        "properties": {"intensity": 0.8, "color": "red"},
    }
    df = _unnest_events([_make_session([event])])
    assert "prop_intensity" in df.columns
    assert "prop_color" in df.columns
    collision_warnings = [
        w for w in recwarn if "collide" in str(w.message)
    ]
    assert len(collision_warnings) == 0
