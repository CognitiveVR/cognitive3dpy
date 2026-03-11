"""Tests for _lookups.py."""

import logging

import httpx
import pytest
import respx
from helpers import load_fixture

import cognitive3dpy._lookups as _lookups_module
from cognitive3dpy._client import BASE_URL
from cognitive3dpy._lookups import (
    _derive_step_detail,
    _describe_event_step,
    _describe_exitpoll_step,
    _describe_gaze_step,
    fetch_objectives_metadata,
    fetch_objects_lookup,
    fetch_questionsets_lookup,
    fetch_scenes_metadata,
)

PROJECT_URL = f"{BASE_URL}/v0/projects/1234"
OBJECTS_URL = f"{BASE_URL}/v0/projects/1234/objects"
OBJECTIVES_URL = f"{BASE_URL}/v0/projects/1234/objectives"
QUESTIONSETS_URL = f"{BASE_URL}/v0/questionSets"


@pytest.fixture(autouse=True)
def clear_caches():
    """Reset lookup caches before each test to ensure isolation."""
    _lookups_module._scenes_cache.clear()
    _lookups_module._objects_cache.clear()
    _lookups_module._objectives_cache.clear()
    _lookups_module._questionsets_cache.clear()
    yield


@respx.mock
def test_fetch_scenes_metadata_caches_result():
    route = respx.get(PROJECT_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    fetch_scenes_metadata(1234)
    fetch_scenes_metadata(1234)
    assert route.call_count == 1


@respx.mock
def test_fetch_objects_lookup_caches_result():
    route = respx.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    fetch_objects_lookup(1234)
    fetch_objects_lookup(1234)
    assert route.call_count == 1


@respx.mock
def test_fetch_objectives_metadata_caches_result():
    route = respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    respx.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    respx.get(QUESTIONSETS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("questionsets.json"))
    )
    fetch_objectives_metadata(1234)
    fetch_objectives_metadata(1234)
    assert route.call_count == 1


@respx.mock
def test_fetch_scenes_metadata_separate_projects_fetch_independently():
    respx.get(f"{BASE_URL}/v0/projects/1234").mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    respx.get(f"{BASE_URL}/v0/projects/9999").mock(
        return_value=httpx.Response(200, json=load_fixture("project.json"))
    )
    fetch_scenes_metadata(1234)
    fetch_scenes_metadata(9999)
    assert 1234 in _lookups_module._scenes_cache
    assert 9999 in _lookups_module._scenes_cache


# ---------------------------------------------------------------------------
# Step description enrichment tests
# ---------------------------------------------------------------------------


def test_describe_event_step_equals():
    comp = {
        "type": "eventstep",
        "eventName": "Equip Hard Hat",
        "occurrenceOperator": "eq",
        "occurrenceValue": 1,
    }
    assert _describe_event_step(comp) == "Event Equip Hard Hat occurs equals 1 time"


def test_describe_event_step_at_least_plural():
    comp = {
        "type": "eventstep",
        "eventName": "Test Voltage",
        "occurrenceOperator": "gte",
        "occurrenceValue": 3,
    }
    assert _describe_event_step(comp) == "Event Test Voltage occurs at least 3 times"


def test_describe_gaze_step_with_object_lookup():
    comp = {
        "type": "gazestep",
        "dynamicObjectIds": ["obj-1"],
        "durationOperator": "gte",
        "durationValue": 0.1,
    }
    lookup = {"obj-1": "Breaker Door"}
    result = _describe_gaze_step(comp, "gazestep", lookup)
    assert result == "Gaze at object Breaker Door for at least 0.1 seconds"


def test_describe_gaze_step_without_lookup_uses_raw_id():
    comp = {
        "type": "gazestep",
        "dynamicObjectIds": ["abc-123"],
        "durationOperator": "gte",
        "durationValue": 0.25,
    }
    result = _describe_gaze_step(comp, "gazestep", None)
    assert result == "Gaze at object abc-123 for at least 0.25 seconds"


def test_describe_fixation_step():
    comp = {
        "type": "fixationstep",
        "dynamicObjectIds": ["obj-2"],
        "durationOperator": "gte",
        "durationValue": 1.0,
    }
    lookup = {"obj-2": "LockoutBox"}
    result = _describe_gaze_step(comp, "fixationstep", lookup)
    assert result == "Fixate on object LockoutBox for at least 1.0 seconds"


def test_describe_mediapoint_step():
    comp = {
        "type": "mediapointstep",
        "dynamicObjectIds": ["obj-1"],
        "durationOperator": "gte",
        "durationValue": 2.0,
    }
    lookup = {"obj-1": "Fire Extinguisher"}
    result = _describe_gaze_step(comp, "mediapointstep", lookup)
    assert result == ("Gaze at media point Fire Extinguisher for at least 2.0 seconds")


def test_describe_exitpoll_step_with_lookup():
    comp = {
        "type": "exitpollstep",
        "exitpollQuestionSetId": "survey:1",
        "clusterIndex": 0,
        "answerOperator": "eq",
        "answerValue": 1,
    }
    qsets = {
        "survey:1": {
            "questions": [
                {"title": "Were you standing?", "type": "BOOLEAN", "answers": []}
            ]
        }
    }
    result = _describe_exitpoll_step(comp, qsets)
    assert result == "Answer to question Were you standing? equals True"


def test_describe_exitpoll_step_fallback_without_lookup():
    comp = {
        "type": "exitpollstep",
        "exitpollQuestionSetId": "assessment_end_survey:1",
        "clusterIndex": 0,
        "answerOperator": "eq",
        "answerValue": 1,
    }
    result = _describe_exitpoll_step(comp, None)
    assert result == "Exitpoll Survey - Question 1 from assessment_end_survey:1"


@respx.mock
@respx.mock
def test_fetch_questionsets_lookup_warns_on_non_dict_response(caplog):
    """When the questionSets endpoint returns a non-dict, a warning is logged."""
    respx.get(QUESTIONSETS_URL).mock(return_value=httpx.Response(200, json=[]))
    with caplog.at_level(logging.WARNING, logger="cognitive3dpy._lookups"):
        result = fetch_questionsets_lookup(1234)
    assert result == {}
    assert any("Expected dict" in m for m in caplog.messages)


@respx.mock
def test_fetch_objectives_metadata_enriches_step_detail():
    respx.get(OBJECTIVES_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objectives.json"))
    )
    respx.get(OBJECTS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("objects.json"))
    )
    respx.get(QUESTIONSETS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("questionsets.json"))
    )
    meta = fetch_objectives_metadata(1234)
    comps = meta["components"]
    steps = comps.filter(comps["is_step"]).sort("step_number")
    details = steps["step_detail"].to_list()
    # Step 1: eventstep for "grab_extinguisher"
    assert details[0] == "Event grab_extinguisher occurs equals 1 time"
    # Step 2: gazestep with obj-1 → "Fire Extinguisher" (from objects.json)
    assert "Fire Extinguisher" in details[1]
    assert "at least" in details[1]


def test_derive_step_detail_unknown_type_returns_none():
    """Unknown step types return None rather than raising."""
    comp = {"type": "unknownstep"}
    assert _derive_step_detail(comp) is None


def test_describe_gaze_step_empty_dynamic_object_ids():
    """Empty dynamicObjectIds falls back to 'Unknown'."""
    comp = {
        "type": "gazestep",
        "dynamicObjectIds": [],
        "durationOperator": "gte",
        "durationValue": 1.0,
    }
    result = _describe_gaze_step(comp, "gazestep", {})
    assert result == "Gaze at object Unknown for at least 1.0 seconds"


def test_describe_exitpoll_step_out_of_bounds_cluster_index():
    """clusterIndex beyond the questions array falls back to raw format."""
    comp = {
        "type": "exitpollstep",
        "exitpollQuestionSetId": "survey:1",
        "clusterIndex": 99,
        "answerOperator": "eq",
        "answerValue": 1,
    }
    qsets = {
        "survey:1": {
            "questions": [
                {"title": "Were you standing?", "type": "boolean", "answers": []}
            ]
        }
    }
    result = _describe_exitpoll_step(comp, qsets)
    assert result == "Exitpoll Survey - Question 100 from survey:1"
