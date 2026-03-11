"""Tests for objectives.py."""

import logging

import httpx
import polars as pl
import pytest
import respx
from helpers import load_fixture

import cognitive3dpy._lookups as _lookups_module
from cognitive3dpy._client import BASE_URL
from cognitive3dpy.objectives import _parse_objective_results, c3d_objective_results

OBJECTIVES_URL = f"{BASE_URL}/v0/projects/1234/objectives"
RESULTS_URL = f"{BASE_URL}/v0/datasets/objectives/objectiveResultQueries"
STEP_RESULTS_URL = f"{BASE_URL}/v0/datasets/objectives/objectiveStepResultQueries"
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


def _mock_objectives_deps(respx_instance):
    """Mock objects and questionsets endpoints for fetch_objectives_metadata."""
    respx_instance.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    respx_instance.get(QUESTIONSETS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("questionsets.json"))
    )


def _mock_base(respx_instance):
    respx_instance.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx_instance)
    respx_instance.post(RESULTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objective_results.json"))
    )


@respx.mock
def test_objective_version_mode_columns():
    _mock_base(respx)
    df = c3d_objective_results(start_date="2025-01-01", end_date="2026-01-01")
    for col in (
        "objective_id",
        "objective_name",
        "objective_version_id",
        "version_number",
        "succeeded",
        "failed",
        "completion_rate",
    ):
        assert col in df.columns, f"Missing: {col}"


def test_invalid_group_by_raises():
    with pytest.raises(ValueError, match="group_by must be"):
        c3d_objective_results(
            group_by="objective",
            start_date="2025-01-01",
            end_date="2026-01-01",
        )


@respx.mock
def test_completion_rate_computed_correctly():
    _mock_base(respx)
    df = c3d_objective_results(start_date="2025-01-01", end_date="2026-01-01")
    # fixture: succeeded=15, failed=5 → rate=0.75
    assert df["completion_rate"][0] == pytest.approx(0.75)


@respx.mock
def test_empty_objectives_returns_empty_dataframe():
    """When the project has no objectives, an empty DataFrame is returned."""
    respx.get(OBJECTIVES_URL).mock(return_value=httpx.Response(200, json=[]))
    _mock_objectives_deps(respx)
    df = c3d_objective_results(
        start_date="2025-01-01", end_date="2026-01-01", warn_empty=False
    )
    assert df.is_empty()


@respx.mock
def test_completion_rate_null_when_total_zero():
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    respx.post(RESULTS_URL).mock(
        return_value=httpx.Response(
            200, json=[{"objectiveVersionId": 100, "succeeded": 0, "failed": 0}]
        )
    )
    df = c3d_objective_results(start_date="2025-01-01", end_date="2026-01-01")
    assert df["completion_rate"][0] is None


@respx.mock
def test_objective_step_mode_columns():
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    respx.post(RESULTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objective_results.json"))
    )
    respx.post(STEP_RESULTS_URL).mock(
        return_value=httpx.Response(
            200, json=load_fixture("objective_step_results.json")
        )
    )
    df = c3d_objective_results(
        group_by="steps",
        start_date="2025-01-01",
        end_date="2026-01-01",
    )
    for col in (
        "step_number",
        "step_type",
        "step_name",
        "succeeded",
        "failed",
        "step_completion_rate",
        "avg_completion_time_s",
        "avg_step_duration_s",
    ):
        assert col in df.columns, f"Missing: {col}"


def test_objective_results_rejects_invalid_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_objective_results(project_id="1234")


def test_objective_results_rejects_invalid_objective_id():
    with pytest.raises(TypeError, match="objective_id must be an int"):
        c3d_objective_results(objective_id="801")


def test_objective_results_rejects_float_objective_id():
    with pytest.raises(TypeError, match="objective_id must be an int"):
        c3d_objective_results(objective_id=801.0)


def test_objective_results_rejects_invalid_objective_version_id():
    with pytest.raises(TypeError, match="objective_version_id must be an int"):
        c3d_objective_results(objective_version_id="1247")


def test_objective_results_rejects_bool_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_objective_results(project_id=True)


def test_objective_results_rejects_bool_objective_id():
    with pytest.raises(TypeError, match="objective_id must be an int"):
        c3d_objective_results(objective_id=False)


def test_objective_results_rejects_bool_objective_version_id():
    with pytest.raises(TypeError, match="objective_version_id must be an int"):
        c3d_objective_results(objective_version_id=True)


@respx.mock
def test_objective_no_stdout_output(capsys):
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    respx.post(RESULTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objective_results.json"))
    )
    c3d_objective_results(start_date="2025-01-01", end_date="2026-01-01")
    assert capsys.readouterr().out == ""


@respx.mock
def test_step_timing_converted_to_seconds():
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    respx.post(RESULTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objective_results.json"))
    )
    respx.post(STEP_RESULTS_URL).mock(
        return_value=httpx.Response(
            200, json=load_fixture("objective_step_results.json")
        )
    )
    df = c3d_objective_results(
        group_by="steps",
        start_date="2025-01-01",
        end_date="2026-01-01",
    )
    # fixture: averageStepCompletionTime=30000ms → 30.0s
    assert df["avg_completion_time_s"][0] == pytest.approx(30.0)
    assert df["avg_step_duration_s"][0] == pytest.approx(15.0)


@respx.mock
def test_step_results_filtered_to_requested_versions():
    """API returns multiple version IDs; only the requested version's steps are kept."""
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    respx.post(RESULTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objective_results.json"))
    )
    # Response contains version 100 (known) and 999 (unknown/extra)
    respx.post(STEP_RESULTS_URL).mock(
        return_value=httpx.Response(
            200,
            json={
                "100": [
                    {
                        "step": 1,
                        "succeeded": 10,
                        "failed": 2,
                        "averageStepCompletionTime": 30000,
                        "averageStepDuration": 15000,
                    }
                ],
                "999": [
                    {
                        "step": 1,
                        "succeeded": 5,
                        "failed": 5,
                        "averageStepCompletionTime": 10000,
                        "averageStepDuration": 5000,
                    }
                ],
            },
        )
    )
    df = c3d_objective_results(
        group_by="steps",
        start_date="2025-01-01",
        end_date="2026-01-01",
    )
    # Only version 100 should be in the output; 999 is not in metadata
    assert set(df["objective_version_id"].to_list()) == {100}
    assert df.shape[0] == 1


@respx.mock
def test_objective_id_filter_excludes_other_objectives():
    """When objective_id is provided with group_by='steps', only that objective's steps are returned."""  # noqa: E501
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    respx.post(RESULTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objective_results.json"))
    )
    respx.post(STEP_RESULTS_URL).mock(
        return_value=httpx.Response(
            200, json=load_fixture("objective_step_results.json")
        )
    )
    df = c3d_objective_results(
        group_by="steps",
        objective_id=1,
        start_date="2025-01-01",
        end_date="2026-01-01",
    )
    assert df["objective_id"].unique().to_list() == [1]


@respx.mock
def test_step_mode_empty_api_response_returns_empty_dataframe():
    """When the step results API returns no data, an empty DataFrame is returned."""
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    _mock_objectives_deps(respx)
    respx.post(RESULTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objective_results.json"))
    )
    respx.post(STEP_RESULTS_URL).mock(
        return_value=httpx.Response(200, json={})
    )
    df = c3d_objective_results(
        group_by="steps",
        start_date="2025-01-01",
        end_date="2026-01-01",
        warn_empty=False,
    )
    assert df.is_empty()


def test_parse_objective_results_warns_on_unmapped_objective_id(caplog):
    versions_meta = pl.DataFrame(
        {
            "objective_version_id": [100, 200],
            "objective_id": [1, 99],
            "version_is_active": [True, True],
            "version_number": [1, 1],
        }
    )
    objectives_meta = pl.DataFrame(
        {
            "objective_id": [1],
            "objective_name": ["Known Objective"],
        }
    )
    metadata = {"versions": versions_meta, "objectives": objectives_meta}
    raw_results = [
        {"objectiveVersionId": 100, "succeeded": 10, "failed": 2},
        {"objectiveVersionId": 200, "succeeded": 5, "failed": 5},
    ]
    with caplog.at_level(logging.WARNING, logger="cognitive3dpy.objectives"):
        result = _parse_objective_results(raw_results, metadata)
    assert result["objective_name"].null_count() == 1
    assert any("unrecognised objective_id" in m for m in caplog.messages)
