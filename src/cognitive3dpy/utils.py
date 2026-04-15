"""User-facing utility functions for cognitive3dpy."""

from __future__ import annotations


def clear_cache(project_id: int | None = None) -> None:
    """Clear cached metadata.

    Parameters
    ----------
    project_id : int, optional
        If provided, clear cache only for this project.
        If ``None`` (default), clear all cached data.
    """
    from cognitive3dpy._lookups import (
        _objectives_cache,
        _objects_cache,
        _questionsets_cache,
        _scenes_cache,
    )
    from cognitive3dpy._schema import _property_types_cache

    caches: list[dict] = [
        _scenes_cache,
        _objects_cache,
        _objectives_cache,
        _questionsets_cache,
        _property_types_cache,
    ]
    if project_id is None:
        for cache in caches:
            cache.clear()
    else:
        for cache in caches:
            cache.pop(project_id, None)
