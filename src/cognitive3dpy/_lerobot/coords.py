"""Coordinate conversion.

Cognitive3D records left-handed, Y-up, metres. Datasets are written right-handed,
Y-up by mirroring X, the convention the Cognitive3D replay viewer uses: positions
become (-x, y, z) and quaternions (qx, -qy, -qz, qw). A mirror M = diag(-1, 1, 1)
maps a rotation R to M R M; a quaternion's vector part transforms as a
pseudovector (det(M) * M v = (x, -y, -z)) and w is unchanged.
"""

from __future__ import annotations

import numpy as np

COORD_SYSTEMS = ("rh_yup", "native")
_RH_YUP = {
    "source": "lh_yup",
    "target": "rh_yup",
    "mirror": "x",
    "units": "meters",
    "quaternion_order": "xyzw",
}
_NATIVE = {**_RH_YUP, "target": "lh_yup", "mirror": None}


def mirror_positions(positions: np.ndarray) -> np.ndarray:
    out = np.array(positions, dtype=np.float64, copy=True)
    out[..., 0] = -out[..., 0]
    return out


def mirror_quaternions(quaternions: np.ndarray) -> np.ndarray:
    out = np.array(quaternions, dtype=np.float64, copy=True)
    out[..., 1] = -out[..., 1]
    out[..., 2] = -out[..., 2]
    return out


def mirror_poses(poses: np.ndarray) -> np.ndarray:
    """(..., 7) arrays of px, py, pz, qx, qy, qz, qw."""
    out = np.array(poses, dtype=np.float64, copy=True)
    out[..., :3] = mirror_positions(out[..., :3])
    out[..., 3:7] = mirror_quaternions(out[..., 3:7])
    return out


def coord_info(coord: str) -> dict:
    if coord not in COORD_SYSTEMS:
        raise ValueError(f"coord must be one of {COORD_SYSTEMS}, got {coord!r}")
    return dict(_RH_YUP if coord == "rh_yup" else _NATIVE)
