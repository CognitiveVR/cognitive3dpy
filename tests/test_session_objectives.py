"""Tests for session_objectives.py."""

import logging

import httpx
import polars as pl
import pytest
import respx
from helpers import load_fixture

import cognitive3dpy._lookups as _lookups_module
from cognitive3dpy._client import BASE_URL
from cognitive3dpy.session_objectives import (
    _unnest_objective_results,
    c3d_session_objectives,
)

SESSIONS_URL = f"{BASE_URL}/v0/datasets/sessions/paginatedListQueries"
PROJECT_URL = f"{BASE_URL}/v0/projects/1234"
OBJECTIVES_URL = f"{BASE_URL}/v0/projects/1234/objectives"
OBJECTS_URL = f"{BASE_URL}/v0/projects/1234/objects"
QUESTIONSETS_URL = f"{BASE_URL}/v0/questionSets"


@pytest.fixture(autouse=True)
def clear_caches():
    """Reset lookup caches before each test to ensure isolation."""
    _lookups_module._scenes_cache.clear()
    _lookups_module._objects_cache.clear()
    _lookups_module._objectives_cache.clear()
    _lookups_module._questionsets_cache.clear()
    yield


def _make_objectives_meta(
    obj_ids: list[str],
    obj_names: list[str],
    ver_ids: list[str],
) -> dict:
    """Build a minimal objectives_meta dict for unit tests."""
    return {
        "objectives": pl.DataFrame(
            {
                "objective_id": obj_ids,
                "objective_name": obj_names,
                "objective_enabled": [True] * len(obj_ids),
            }
        ),
        "versions": pl.DataFrame(
            {
                "objective_version_id": ver_ids,
                "objective_id": obj_ids,
                "version_is_active": [True] * len(ver_ids),
                "version_number": list(range(1, len(ver_ids) + 1)),
            }
        ),
        "components": pl.DataFrame(
            schema={
                "objective_version_id": pl.Utf8,
                "step_number": pl.Int64,
                "step_type": pl.Utf8,
                "step_detail": pl.Utf8,
                "step_name": pl.Utf8,
                "is_step": pl.Boolean,
            }
        ),
    }


def _mock_objectives_deps(respx_instance):
    """Mock objects and questionsets endpoints for fetch_objectives_metadata."""
    respx_instance.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    respx_instance.get(QUESTIONSETS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("questionsets.json"))
    )


def _mock_base(respx_instance):
    """Mock the project, objectives, and sessions endpoints."""
    respx_instance.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    respx_instance.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx_instance)
    respx_instance.post(SESSIONS_URL).mock(
        return_value=httpx.Response(
            200, json=load_fixture("sessions_with_objectives.json")
        )
    )


@respx.mock
def test_happy_path_returns_correct_shape_and_columns():
    _mock_base(respx)
    df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")

    expected_cols = {
        "project_id",
        "scene_id",
        "scene_name",
        "scene_version_id",
        "session_id",
        "participant_id",
        "session_date",
        "objective_id",
        "objective_name",
        "step_number",
        "step_description",
        "step_timestamp",
        "step_duration",
        "step_duration_sec",
        "step_result",
    }
    assert expected_cols.issubset(set(df.columns))
    # Fixture has 1 scene with 2 versions; each returns 1 session with 2 steps
    assert df.shape[0] == 4


@respx.mock
def test_session_id_and_participant_populated():
    _mock_base(respx)
    df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    assert df["session_id"][0] == "sess-001"
    assert df["participant_id"][0] == "user-42"


@respx.mock
def test_step_result_values():
    _mock_base(respx)
    df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    results = df["step_result"].to_list()
    # 2 versions × 2 steps each: succeeded/failed repeated
    assert results == ["succeeded", "failed", "succeeded", "failed"]


@respx.mock
def test_duration_conversion():
    _mock_base(respx)
    df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    # First step: duration=12500ms → 12.5s
    assert df["step_duration"][0] == 12500
    assert df["step_duration_sec"][0] == pytest.approx(12.5)
    # Second step: duration=8200ms → 8.2s
    assert df["step_duration"][1] == 8200
    assert df["step_duration_sec"][1] == pytest.approx(8.2)


@respx.mock
def test_step_timestamp_is_datetime():
    _mock_base(respx)
    df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    assert df.schema["step_timestamp"] == pl.Datetime("ms", "UTC")


@respx.mock
def test_session_date_is_datetime():
    _mock_base(respx)
    df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    assert df.schema["session_date"] == pl.Datetime("us", "UTC")


@respx.mock
def test_objective_name_populated():
    _mock_base(respx)
    df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    # objectives.json has id=1/version_id=100, name="Fire Safety"
    assert df["objective_name"][0] == "Fire Safety"


@respx.mock
def test_step_description_populated():
    _mock_base(respx)
    df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    # objectives.json step 1 name = "Grab Extinguisher"
    assert df["step_description"][0] == "Grab Extinguisher"


@respx.mock
def test_empty_objectives_returns_empty_dataframe():
    """When the project has no objectives, an empty DataFrame is returned."""
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    respx.get(OBJECTIVES_URL).mock(return_value=httpx.Response(200, json=[]))
    _mock_objectives_deps(respx)
    df = c3d_session_objectives(
        start_date="2025-01-01", end_date="2026-01-01", warn_empty=False
    )
    assert df.is_empty()


@respx.mock
def test_empty_sessions_returns_empty_dataframe():
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(200, json={"count": 0, "pages": 0, "results": []})
    )
    df = c3d_session_objectives(
        start_date="2025-01-01", end_date="2026-01-01", warn_empty=False
    )
    assert df.is_empty()


@respx.mock
def test_no_objective_results_key_returns_empty():
    """Sessions exist but have no objectiveResults."""
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "count": 1,
                "pages": 1,
                "results": [
                    {
                        "sessionId": "sess-002",
                        "projectId": 1234,
                        "participantId": "user-1",
                        "date": "2025-06-15T14:30:00Z",
                    }
                ],
            },
        )
    )
    df = c3d_session_objectives(
        start_date="2025-01-01", end_date="2026-01-01", warn_empty=False
    )
    assert df.is_empty()


def test_stale_version_keys_skipped():
    """objectiveResults keys that don't match any version or objective are skipped."""
    step_ok = {
        "step": 1,
        "timestamp": 1000,
        "duration": 500,
        "result": "succeeded",
    }
    step_fail = {
        "step": 1,
        "timestamp": 2000,
        "duration": 600,
        "result": "failed",
    }
    sessions = [
        {
            "sessionId": "sess-001",
            "participantId": "user-1",
            "date": "2025-06-15T14:30:00Z",
            "objectiveResults": {
                "999": [step_ok],
                "100": [step_fail],
            },
        }
    ]

    rows = _unnest_objective_results(
        sessions=sessions,
        project_id=1234,
        scene_id="scene-abc",
        scene_name="Test",
        scene_version_id=10,
        obj_name_map={"1": "Known"},
        version_lookup={"100": {"objective_id": "1", "objective_name": "Known"}},
        step_desc_map={},
    )

    # Only version 100 should match; 999 is stale
    assert len(rows) == 1
    assert rows[0]["step_result"] == "failed"


def test_objective_id_fallback():
    """When key doesn't match a version ID, fall back to objective ID lookup."""
    step = {
        "step": 1,
        "timestamp": 1000,
        "duration": 500,
        "result": "succeeded",
    }
    sessions = [
        {
            "sessionId": "sess-001",
            "participantId": "user-1",
            "date": "2025-06-15T14:30:00Z",
            "objectiveResults": {
                # Key "1" matches objective_id, not version_id
                "1": [step],
            },
        }
    ]

    rows = _unnest_objective_results(
        sessions=sessions,
        project_id=1234,
        scene_id="scene-abc",
        scene_name="Test",
        scene_version_id=10,
        obj_name_map={"1": "Fallback Obj"},
        version_lookup={"100": {"objective_id": "1", "objective_name": "Fallback Obj"}},
        step_desc_map={},
    )

    assert len(rows) == 1
    assert rows[0]["objective_name"] == "Fallback Obj"
    assert rows[0]["objective_id"] == "1"


@respx.mock
def test_output_pandas():
    pytest.importorskip("pyarrow")
    pd = __import__("pandas")
    _mock_base(respx)
    df = c3d_session_objectives(
        start_date="2025-01-01", end_date="2026-01-01", output="pandas"
    )
    assert isinstance(df, pd.DataFrame)


@respx.mock
def test_multiple_scenes_combined():
    """Sessions from multiple scene versions are combined."""
    project_two_scenes = {
        "id": 1234,
        "name": "Test Project",
        "scenes": [
            {
                "id": "scene-abc",
                "sceneName": "Room A",
                "versions": [{"id": 10, "versionNumber": 1}],
            },
            {
                "id": "scene-def",
                "sceneName": "Room B",
                "versions": [{"id": 20, "versionNumber": 1}],
            },
        ],
    }
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=project_two_scenes)
    )
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    respx.post(SESSIONS_URL).mock(
        return_value=httpx.Response(
            200, json=load_fixture("sessions_with_objectives.json")
        )
    )
    df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    # 2 scenes × 2 steps each = 4 rows
    assert df.shape[0] == 4
    assert set(df["scene_name"].to_list()) == {"Room A", "Room B"}


def test_rejects_invalid_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_session_objectives(project_id="1234")


def test_rejects_bool_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_session_objectives(project_id=True)


def test_rejects_invalid_n():
    with pytest.raises(ValueError, match="n must be a positive integer"):
        c3d_session_objectives(n=0)


def test_rejects_invalid_scene_id():
    with pytest.raises(TypeError, match="scene_id must be a str"):
        c3d_session_objectives(scene_id=12345)


def test_rejects_invalid_scene_version_id():
    with pytest.raises(TypeError, match="scene_version_id must be an int"):
        c3d_session_objectives(scene_version_id="1")


@respx.mock
def test_no_stdout_output(capsys):
    _mock_base(respx)
    c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    assert capsys.readouterr().out == ""


@respx.mock
def test_failed_scene_version_logs_error_and_returns_partial_results(caplog):
    """If one scene version fails, the error is logged and partial results returned."""
    project_two_scenes = {
        "id": 1234,
        "name": "Test Project",
        "scenes": [
            {
                "id": "scene-abc",
                "sceneName": "Room A",
                "versions": [{"id": 10, "versionNumber": 1}],
            },
            {
                "id": "scene-def",
                "sceneName": "Room B",
                "versions": [{"id": 20, "versionNumber": 1}],
            },
        ],
    }
    respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=project_two_scenes)
    )
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    # First call succeeds, second raises a server error
    respx.post(SESSIONS_URL).mock(
        side_effect=[
            httpx.Response(200, json=load_fixture("sessions_with_objectives.json")),
            httpx.Response(500),
        ]
    )
    with caplog.at_level(logging.ERROR, logger="cognitive3dpy.session_objectives"):
        df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    # Results from the successful version are still returned
    assert df.shape[0] > 0
    assert any("Failed to fetch scene version" in m for m in caplog.messages)


def test_step_description_fallback_to_step_detail():
    """When step_name is None but step_detail exists, step_detail is used."""
    step = {"step": 1, "timestamp": 1000, "duration": 500, "result": "succeeded"}
    sessions = [
        {
            "sessionId": "sess-001",
            "participantId": "user-1",
            "date": "2025-06-15T14:30:00Z",
            "objectiveResults": {"100": [step]},
        }
    ]
    # step_desc_map has step_detail value (no step_name)
    rows = _unnest_objective_results(
        sessions=sessions,
        project_id=1234,
        scene_id="scene-abc",
        scene_name="Test",
        scene_version_id=10,
        obj_name_map={"1": "Known Obj"},
        version_lookup={"100": {"objective_id": "1", "objective_name": "Known Obj"}},
        step_desc_map={("100", 1): "Event grab_extinguisher occurs equals 1 time"},
    )
    assert len(rows) == 1
    assert rows[0]["step_description"] == "Event grab_extinguisher occurs equals 1 time"


def test_step_description_empty_when_no_match():
    """When no entry in step_desc_map, step_description is empty string."""
    step = {"step": 99, "timestamp": 1000, "duration": 500, "result": "succeeded"}
    sessions = [
        {
            "sessionId": "sess-001",
            "participantId": "user-1",
            "date": "2025-06-15T14:30:00Z",
            "objectiveResults": {"100": [step]},
        }
    ]
    rows = _unnest_objective_results(
        sessions=sessions,
        project_id=1234,
        scene_id="scene-abc",
        scene_name="Test",
        scene_version_id=10,
        obj_name_map={"1": "Known Obj"},
        version_lookup={"100": {"objective_id": "1", "objective_name": "Known Obj"}},
        step_desc_map={},
    )
    assert len(rows) == 1
    assert rows[0]["step_description"] == ""


@respx.mock
def test_numeric_step_timestamp_converted_to_datetime():
    """Integer epoch-ms step_timestamp is converted to Datetime(ms, UTC)."""
    _mock_base(respx)
    df = c3d_session_objectives(start_date="2025-01-01", end_date="2026-01-01")
    # sessions_with_objectives.json uses integer timestamps
    assert df.schema["step_timestamp"] == pl.Datetime("ms", "UTC")
