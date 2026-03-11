"""Tests for exitpoll.py."""

import logging

import httpx
import pytest
import respx
from helpers import load_fixture

from cognitive3dpy._client import BASE_URL
from cognitive3dpy.exitpoll import (
    _WARNED_QUESTION_TYPES,
    _map_value_label,
    c3d_exitpoll,
)

HOOKS_URL = f"{BASE_URL}/v0/questionSets?projectIds=1234"
RESPONSES_URL = (
    f"{BASE_URL}/v0/projects/1234/questionSets/survey/1/responseCountQueries"
)


def _mock_exitpoll(respx_instance):
    respx_instance.get(HOOKS_URL).mock(
        return_value=httpx.Response(200, json=load_fixture("questionsets.json"))
    )
    respx_instance.post(RESPONSES_URL).mock(
        return_value=httpx.Response(
            200, json=load_fixture("questionset_responses.json")
        )
    )


@respx.mock
def test_exitpoll_expected_columns():
    _mock_exitpoll(respx)
    df = c3d_exitpoll()
    for col in (
        "hook",
        "version",
        "question_index",
        "question_title",
        "question_type",
        "value",
        "value_label",
        "count",
    ):
        assert col in df.columns, f"Missing: {col}"


@respx.mock
def test_exitpoll_question_types_lowercased():
    _mock_exitpoll(respx)
    df = c3d_exitpoll()
    types = df["question_type"].unique().to_list()
    assert all(t == t.lower() for t in types)


@respx.mock
def test_exitpoll_skipped_responses_labelled():
    _mock_exitpoll(respx)
    df = c3d_exitpoll()
    skipped = df.filter(df["value"] == "skipped")
    assert skipped.shape[0] > 0
    assert all(v == "skipped" for v in skipped["value_label"].to_list())


@respx.mock
def test_exitpoll_count_non_negative():
    _mock_exitpoll(respx)
    df = c3d_exitpoll()
    assert df["count"].min() >= 0


@respx.mock
def test_exitpoll_question_index_starts_at_one():
    _mock_exitpoll(respx)
    df = c3d_exitpoll()
    assert df["question_index"].min() >= 1


@respx.mock
def test_exitpoll_hook_filter():
    _mock_exitpoll(respx)
    df = c3d_exitpoll(hook="survey")
    assert df["hook"].unique().to_list() == ["survey"]


@respx.mock
def test_exitpoll_unknown_hook_returns_empty():
    _mock_exitpoll(respx)
    df = c3d_exitpoll(hook="nonexistent", warn_empty=False)
    assert df.is_empty()


def test_exitpoll_rejects_string_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_exitpoll(project_id="1234")


def test_exitpoll_rejects_float_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_exitpoll(project_id=1234.0)


def test_exitpoll_rejects_bool_project_id():
    with pytest.raises(TypeError, match="project_id must be an int"):
        c3d_exitpoll(project_id=True)


# --- _map_value_label unit tests (no HTTP needed) ---


def test_map_boolean():
    assert _map_value_label("0", "boolean", []) == "False"
    assert _map_value_label("1", "boolean", []) == "True"


def test_map_happysad():
    assert _map_value_label("0", "happysad", []) == "Sad"
    assert _map_value_label("1", "happysad", []) == "Happy"


def test_map_thumbs():
    assert _map_value_label("0", "thumbs", []) == "Down"
    assert _map_value_label("1", "thumbs", []) == "Up"


def test_map_scale_returns_value():
    assert _map_value_label("7", "scale", []) == "7"


def test_map_multiple_choice():
    answers = [{"answer": "Great"}, {"answer": "Okay"}, {"answer": "Poor"}]
    assert _map_value_label("0", "multiple", answers) == "Great"
    assert _map_value_label("2", "multiple", answers) == "Poor"


def test_map_multiple_out_of_range():
    answers = [{"answer": "Yes"}]
    assert _map_value_label("5", "multiple", answers) == "5"


def test_map_voice():
    assert _map_value_label("0", "voice", []) == "Responded"


def test_map_unknown_type_returns_value():
    assert _map_value_label("42", "unknown_type", []) == "42"


def test_map_unknown_type_logs_warning(caplog):
    _WARNED_QUESTION_TYPES.discard("unknown_type")
    with caplog.at_level(logging.WARNING, logger="cognitive3dpy.exitpoll"):
        _map_value_label("42", "unknown_type", [])
    assert any("Unrecognised question type" in m for m in caplog.messages)


def test_map_unknown_type_warns_only_once(caplog):
    _WARNED_QUESTION_TYPES.discard("once_type")
    with caplog.at_level(logging.WARNING, logger="cognitive3dpy.exitpoll"):
        _map_value_label("1", "once_type", [])
        _map_value_label("2", "once_type", [])
        _map_value_label("3", "once_type", [])
    matches = [m for m in caplog.messages if "once_type" in m]
    assert len(matches) == 1
