"""Lookup helpers for scenes, objects, and objectives metadata."""

from __future__ import annotations

import logging

import polars as pl

from cognitive3dpy._client import c3d_get

logger = logging.getLogger(__name__)

_scenes_cache: dict[int, dict] = {}
_objects_cache: dict[int, dict[str, str]] = {}
_objectives_cache: dict[int, dict] = {}
_questionsets_cache: dict[int, dict] = {}


def fetch_scenes_metadata(project_id: int) -> dict:
    """Fetch scene and version metadata for a project.

    Parameters
    ----------
    project_id : int
        The Cognitive3D project ID.

    Returns
    -------
    dict
        ``"versions"`` — a :class:`polars.DataFrame` with columns
        ``scene_id``, ``scene_name``, ``version_id``, ``version_number``.

        ``"lookup"`` — a ``dict[str, str]`` mapping ``str(version_id)``
        to ``scene_name``.
    """
    if project_id in _scenes_cache:
        return _scenes_cache[project_id]

    data = c3d_get(f"/v0/projects/{project_id}")
    scenes = data.get("scenes", [])

    rows: list[dict] = []
    lookup: dict[str, str] = {}

    for scene in scenes:
        scene_id = scene["id"]
        scene_name = scene["sceneName"]
        for ver in scene.get("versions", []):
            version_id = ver["id"]
            rows.append(
                {
                    "scene_id": scene_id,
                    "scene_name": scene_name,
                    "version_id": version_id,
                    "version_number": ver["versionNumber"],
                }
            )
            lookup[str(version_id)] = scene_name

    versions_df = (
        pl.DataFrame(rows)
        if rows
        else pl.DataFrame(
            schema={
                "scene_id": pl.Int64,
                "scene_name": pl.Utf8,
                "version_id": pl.Int64,
                "version_number": pl.Int64,
            }
        )
    )

    result = {"versions": versions_df, "lookup": lookup}
    _scenes_cache[project_id] = result
    return result


def fetch_objects_lookup(project_id: int) -> dict[str, str]:
    """Fetch the dynamic objects lookup for a project.

    Parameters
    ----------
    project_id : int
        The Cognitive3D project ID.

    Returns
    -------
    dict[str, str]
        Mapping of ``sdkId`` to object ``name``.
    """
    if project_id in _objects_cache:
        return _objects_cache[project_id]

    data = c3d_get(f"/v0/projects/{project_id}/objects")
    result = (
        {}
        if not data
        else {obj["sdkId"]: obj["name"] for obj in data if obj.get("sdkId") is not None}
    )
    _objects_cache[project_id] = result
    return result


def fetch_questionsets_lookup(project_id: int) -> dict:
    """Fetch question set metadata for a project.

    Parameters
    ----------
    project_id : int
        The Cognitive3D project ID.

    Returns
    -------
    dict
        Mapping of ``"{hook}:{version}"`` to a dict with ``"questions"``
        (list of dicts with ``"title"``, ``"type"``, ``"answers"``).
    """
    if project_id in _questionsets_cache:
        return _questionsets_cache[project_id]

    data = c3d_get(f"/v0/questionSets?projectIds={project_id}")
    if not isinstance(data, dict):
        logger.warning(
            "Expected dict from questionSets endpoint, got %s — "
            "exitpoll step descriptions will use fallback format.",
            type(data).__name__,
        )
        result = {}
    else:
        result = data
    _questionsets_cache[project_id] = result
    return result


_OPERATOR_LABELS: dict[str, str] = {
    "eq": "equals",
    "gte": "at least",
    "lte": "at most",
    "gt": "greater than",
    "lt": "less than",
}


def _derive_step_detail(
    comp: dict,
    objects_lookup: dict[str, str] | None = None,
    questionsets_lookup: dict | None = None,
) -> str | None:
    """Compose a human-readable step description from an objective component.

    When *objects_lookup* or *questionsets_lookup* are provided, dynamic object
    IDs and exitpoll question set IDs are resolved to friendly names.
    """
    step_type = comp.get("type", "")

    if step_type == "eventstep":
        return _describe_event_step(comp)
    if step_type == "exitpollstep":
        return _describe_exitpoll_step(comp, questionsets_lookup)
    if step_type in ("gazestep", "fixationstep", "mediapointstep"):
        return _describe_gaze_step(comp, step_type, objects_lookup)
    return None


def _describe_event_step(comp: dict) -> str:
    """Build description for an event step."""
    name = comp.get("eventName") or "Unknown"
    op = _OPERATOR_LABELS.get(comp.get("occurrenceOperator", ""), "")
    count = comp.get("occurrenceValue", "")
    suffix = "time" if count == 1 else "times"
    return f"Event {name} occurs {op} {count} {suffix}"


def _describe_gaze_step(
    comp: dict,
    step_type: str,
    objects_lookup: dict[str, str] | None,
) -> str:
    """Build description for gaze, fixation, or media point steps."""
    ids = comp.get("dynamicObjectIds", [])
    if objects_lookup:
        names = [objects_lookup.get(i, i) for i in ids]
    else:
        names = list(ids)
    target = ", ".join(names) if names else "Unknown"

    dur_op = _OPERATOR_LABELS.get(comp.get("durationOperator", ""), "")
    dur_val = comp.get("durationValue", "")

    type_labels = {
        "gazestep": "Gaze at object",
        "fixationstep": "Fixate on object",
        "mediapointstep": "Gaze at media point",
    }
    label = type_labels.get(step_type, step_type)
    return f"{label} {target} for {dur_op} {dur_val} seconds"


def _describe_exitpoll_step(
    comp: dict,
    questionsets_lookup: dict | None,
) -> str:
    """Build description for an exitpoll step."""
    qset_id = comp.get("exitpollQuestionSetId", "")
    cluster_idx = comp.get("clusterIndex", 0)
    answer_op = _OPERATOR_LABELS.get(comp.get("answerOperator", ""), "")
    answer_val = comp.get("answerValue")

    question_title = None
    answer_label = str(answer_val) if answer_val is not None else ""

    if questionsets_lookup and qset_id in questionsets_lookup:
        qset = questionsets_lookup[qset_id]
        questions = qset.get("questions", [])
        if 0 <= cluster_idx < len(questions):
            q = questions[cluster_idx]
            question_title = (q.get("title") or "").strip()
            q_type = (q.get("type") or "").lower()
            if answer_val is not None:
                answer_label = _map_exitpoll_value(
                    str(answer_val), q_type, q.get("answers", [])
                )

    if question_title:
        return f"Answer to question {question_title} {answer_op} {answer_label}"

    # Fallback: use raw question set ID
    q_num = cluster_idx + 1
    return f"Exitpoll Survey - Question {q_num} from {qset_id}"


def _map_exitpoll_value(value: str, question_type: str, answers: list[dict]) -> str:
    """Map a raw exitpoll answer value to a human-readable label."""
    if question_type == "boolean":
        return {"0": "False", "1": "True"}.get(value, value)
    if question_type == "happysad":
        return {"0": "Sad", "1": "Happy"}.get(value, value)
    if question_type == "thumbs":
        return {"0": "Down", "1": "Up"}.get(value, value)
    if question_type == "multiple":
        try:
            idx = int(value)
            return str(answers[idx].get("answer", value))
        except (ValueError, IndexError):
            return value
    return value


def fetch_objectives_metadata(project_id: int) -> dict:
    """Fetch objectives, versions, and component metadata for a project.

    Parameters
    ----------
    project_id : int
        The Cognitive3D project ID.

    Returns
    -------
    dict
        ``"objectives"`` — :class:`polars.DataFrame` with columns
        ``objective_id``, ``objective_name``, ``objective_enabled``.

        ``"versions"`` — :class:`polars.DataFrame` with columns
        ``objective_version_id``, ``objective_id``, ``version_is_active``,
        ``version_number``.

        ``"components"`` — :class:`polars.DataFrame` with columns
        ``objective_version_id``, ``step_number``, ``step_type``,
        ``step_detail``, ``step_name``, ``is_step``.
    """
    if project_id in _objectives_cache:
        return _objectives_cache[project_id]

    data = c3d_get(f"/v0/projects/{project_id}/objectives")
    if not data:
        data = []

    objects_lookup = fetch_objects_lookup(project_id)
    questionsets_lookup = fetch_questionsets_lookup(project_id)

    obj_rows: list[dict] = []
    ver_rows: list[dict] = []
    comp_rows: list[dict] = []

    for obj in data:
        objective_id = obj["id"]
        obj_rows.append(
            {
                "objective_id": objective_id,
                "objective_name": obj["name"],
                "objective_enabled": obj.get("enabled", True),
            }
        )

        versions = obj.get("objectiveVersions", [])
        # Rank version IDs ascending within each objective.
        sorted_ver_ids = sorted(ver["id"] for ver in versions)
        ver_rank = {vid: i + 1 for i, vid in enumerate(sorted_ver_ids)}

        for ver in versions:
            ver_id = ver["id"]
            ver_rows.append(
                {
                    "objective_version_id": ver_id,
                    "objective_id": objective_id,
                    "version_is_active": ver.get("isActive", False),
                    "version_number": ver_rank[ver_id],
                }
            )

            for comp in ver.get("objectiveComponents", []):
                comp_rows.append(
                    {
                        "objective_version_id": ver_id,
                        "step_number": comp.get("sequenceNumber"),
                        "step_type": comp.get("type"),
                        "step_detail": _derive_step_detail(
                            comp, objects_lookup, questionsets_lookup
                        ),
                        "step_name": comp.get("name"),
                        "is_step": comp.get("isStep", False),
                    }
                )

    objectives_df = (
        pl.DataFrame(obj_rows)
        if obj_rows
        else pl.DataFrame(
            schema={
                "objective_id": pl.Int64,
                "objective_name": pl.Utf8,
                "objective_enabled": pl.Boolean,
            }
        )
    )
    versions_df = (
        pl.DataFrame(ver_rows)
        if ver_rows
        else pl.DataFrame(
            schema={
                "objective_version_id": pl.Int64,
                "objective_id": pl.Int64,
                "version_is_active": pl.Boolean,
                "version_number": pl.Int64,
            }
        )
    )
    components_df = (
        pl.DataFrame(comp_rows)
        if comp_rows
        else pl.DataFrame(
            schema={
                "objective_version_id": pl.Int64,
                "step_number": pl.Int64,
                "step_type": pl.Utf8,
                "step_detail": pl.Utf8,
                "step_name": pl.Utf8,
                "is_step": pl.Boolean,
            }
        )
    )

    result = {
        "objectives": objectives_df,
        "versions": versions_df,
        "components": components_df,
    }
    _objectives_cache[project_id] = result
    return result
