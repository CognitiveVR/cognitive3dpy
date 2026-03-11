"""cognitive3dpy — Python client for the Cognitive3D Analytics REST API."""

from importlib.metadata import version

__version__ = version("cognitive3dpy")

from cognitive3dpy._client import (
    C3DAPIError,
    C3DAuthError,
    C3DError,
    C3DNotFoundError,
    c3d_set_timeout,
)
from cognitive3dpy.auth import c3d_auth, c3d_project
from cognitive3dpy.events import c3d_events
from cognitive3dpy.exitpoll import c3d_exitpoll
from cognitive3dpy.objectives import c3d_objective_results
from cognitive3dpy.session_objectives import c3d_session_objectives
from cognitive3dpy.sessions import c3d_sessions

__all__ = [
    # Auth
    "c3d_auth",
    "c3d_project",
    # Data functions
    "c3d_sessions",
    "c3d_events",
    "c3d_objective_results",
    "c3d_session_objectives",
    "c3d_exitpoll",
    # Config
    "c3d_set_timeout",
    # Exceptions
    "C3DError",
    "C3DAuthError",
    "C3DNotFoundError",
    "C3DAPIError",
]
