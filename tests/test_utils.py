"""Tests for user-facing utility functions."""

from cognitive3dpy._lookups import (
    _objectives_cache,
    _objects_cache,
    _questionsets_cache,
    _scenes_cache,
)
from cognitive3dpy._schema import _property_types_cache
from cognitive3dpy.utils import clear_cache

ALL_CACHES = [
    _scenes_cache,
    _objects_cache,
    _objectives_cache,
    _questionsets_cache,
    _property_types_cache,
]


def _populate_caches():
    """Insert dummy entries for two projects into all caches."""
    _scenes_cache[1] = {"versions": [], "lookup": {}}
    _scenes_cache[2] = {"versions": [], "lookup": {}}
    _objects_cache[1] = {"obj1": "Cube"}
    _objects_cache[2] = {"obj2": "Sphere"}
    _objectives_cache[1] = {"objectives": []}
    _objectives_cache[2] = {"objectives": []}
    _questionsets_cache[1] = {}
    _questionsets_cache[2] = {}
    _property_types_cache[1] = {}
    _property_types_cache[2] = {}


def _clear_all():
    for cache in ALL_CACHES:
        cache.clear()


def test_clear_cache_all(self=None):
    _populate_caches()
    clear_cache()
    for cache in ALL_CACHES:
        assert len(cache) == 0
    _clear_all()


def test_clear_cache_single_project():
    _populate_caches()
    clear_cache(project_id=1)
    for cache in ALL_CACHES:
        assert 1 not in cache
        assert 2 in cache
    _clear_all()


def test_clear_cache_nonexistent_project():
    _populate_caches()
    clear_cache(project_id=999)
    for cache in ALL_CACHES:
        assert len(cache) == 2
    _clear_all()
