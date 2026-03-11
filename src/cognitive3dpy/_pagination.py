"""Session pagination for the Cognitive3D API."""

from __future__ import annotations

import logging

from cognitive3dpy._client import c3d_request

logger = logging.getLogger(__name__)

_ABSOLUTE_MAX_SESSIONS = 500
_MAX_PAGE_LIMIT = 100


def paginate_sessions(
    project_id: int,
    session_filters: list[dict],
    max_sessions: int = 500,
    page_limit: int = 100,
    include_events: bool = False,
    include_all_objective_data: bool = False,
    session_type: str = "project",
    scene_id: int | None = None,
    version_id: int | None = None,
) -> list[dict]:
    """Fetch sessions page-by-page up to *max_sessions*.

    Parameters
    ----------
    project_id : int
        The Cognitive3D project ID.
    session_filters : list[dict]
        Filter array from :func:`build_session_filters`.
    max_sessions : int
        Maximum total sessions to return. Cannot exceed
        ``_ABSOLUTE_MAX_SESSIONS`` (500).
    page_limit : int
        Items per API page. Cannot exceed ``_MAX_PAGE_LIMIT`` (100).
    include_events : bool
        If True, the API response includes an events array per session.
    include_all_objective_data : bool
        If True, the API response includes per-step objective results
        per session. Only effective with ``session_type="scene"``.
    session_type : str
        ``"project"`` or ``"scene"``.
    scene_id : int, optional
        Required for scene mode.
    version_id : int, optional
        Required for scene mode.

    Returns
    -------
    list[dict]
        Flat list of session result dicts.
    """
    if max_sessions > _ABSOLUTE_MAX_SESSIONS:
        raise ValueError(
            f"max_sessions ({max_sessions}) exceeds the absolute limit of "
            f"{_ABSOLUTE_MAX_SESSIONS}."
        )
    if page_limit > _MAX_PAGE_LIMIT:
        raise ValueError(
            f"page_limit ({page_limit}) exceeds the maximum of {_MAX_PAGE_LIMIT}."
        )

    all_results: list[dict] = []
    remaining = max_sessions
    page = 0
    total_available: int | None = None

    while remaining > 0:
        page_size = min(remaining, page_limit)

        entity_filters: dict = {"projectId": project_id}
        if session_type == "scene":
            if scene_id is not None:
                entity_filters["sceneId"] = scene_id
            if version_id is not None:
                entity_filters["versionId"] = version_id

        body: dict = {
            "page": page,
            "limit": page_size,
            "sort": "desc",
            "orderBy": {"fieldName": "date", "fieldParent": "session"},
            "entityFilters": entity_filters,
            "sessionFilters": session_filters,
            "eventFilters": [],
            "objectiveFilters": [],
            "userFilters": [],
            "sessionType": session_type,
            "includeEvents": include_events,
            "includeAllObjectiveData": include_all_objective_data,
        }

        response = c3d_request("/v0/datasets/sessions/paginatedListQueries", body)

        if total_available is None:
            total_available = response.get("count", 0)

        results = response.get("results", [])
        if not results:
            break

        all_results.extend(results)
        remaining -= len(results)

        logger.info(
            "Fetching sessions... %d / %d",
            len(all_results),
            min(max_sessions, total_available),
        )

        total_pages = response.get("pages", 1)
        if remaining <= 0 or page + 1 >= total_pages:
            break

        page += 1

    return all_results[:max_sessions]
