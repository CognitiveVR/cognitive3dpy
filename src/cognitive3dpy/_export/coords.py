"""Coordinate conversion.

Cognitive3D records left-handed, Y-up, metres (x right, y up, z forward). An
export converts with an orthogonal matrix C:

- ``rh_yup``: right-handed Y-up by mirroring X, the convention the Cognitive3D
  replay viewer uses: positions become (-x, y, z), quaternions (qx, -qy, -qz, qw).
- ``ros``: REP-103 (x forward, y left, z up), what ROS, Isaac Lab and most robot
  datasets use: positions become (z, -x, y), quaternions (-qz, qx, -qy, qw).
- ``native``: as recorded.

A rotation R becomes C R C^T; a quaternion's vector part transforms as a
pseudovector (det(C) * C v) and w is unchanged.
"""

from __future__ import annotations

import numpy as np

COORD_SYSTEMS = ("rh_yup", "ros", "native")
_INFO = {
    "rh_yup": {"target": "rh_yup", "mirror": "x", "up": "+y"},
    "ros": {"target": "ros_rep103", "mirror": None, "up": "+z", "forward": "+x"},
    "native": {"target": "lh_yup", "mirror": None, "up": "+y"},
}
# Local beam direction of laserscan beam i, θ = angle_min + i·angle_increment.
# Under ros the scan pose is the scan plane's frame, as in sensor_msgs/LaserScan.
LASERSCAN_BEAM = {
    "rh_yup": "(-r cos θ, r sin θ, 0)",
    "ros": "(r cos θ, r sin θ, 0)",
    "native": "(r cos θ, r sin θ, 0)",
}
# C·diag(1, 1, -1) for ros: keeps the recorded scan plane (local xy) as the
# frame's xy plane while staying a proper rotation.
_ROS_SCAN_PLANE = np.array([0.5, -0.5, -0.5, 0.5])


def _check(coord: str) -> None:
    if coord not in COORD_SYSTEMS:
        raise ValueError(f"coord must be one of {COORD_SYSTEMS}, got {coord!r}")


def convert_positions(positions: np.ndarray, coord: str) -> np.ndarray:
    _check(coord)
    p = np.array(positions, dtype=np.float64, copy=True)
    if coord == "rh_yup":
        p[..., 0] = -p[..., 0]
    elif coord == "ros":
        x, y, z = p[..., 0].copy(), p[..., 1].copy(), p[..., 2].copy()
        p[..., 0], p[..., 1], p[..., 2] = z, -x, y
    return p


def convert_quaternions(quaternions: np.ndarray, coord: str) -> np.ndarray:
    """(..., 4) arrays of qx, qy, qz, qw."""
    _check(coord)
    q = np.array(quaternions, dtype=np.float64, copy=True)
    if coord == "rh_yup":
        q[..., 1] = -q[..., 1]
        q[..., 2] = -q[..., 2]
    elif coord == "ros":
        x, y, z = q[..., 0].copy(), q[..., 1].copy(), q[..., 2].copy()
        q[..., 0], q[..., 1], q[..., 2] = -z, x, -y
    return q


def convert_poses(poses: np.ndarray, coord: str) -> np.ndarray:
    """(..., 7) arrays of px, py, pz, qx, qy, qz, qw."""
    out = np.array(poses, dtype=np.float64, copy=True)
    out[..., :3] = convert_positions(out[..., :3], coord)
    out[..., 3:7] = convert_quaternions(out[..., 3:7], coord)
    return out


def convert_scan_poses(poses: np.ndarray, coord: str) -> np.ndarray:
    """Laserscan poses; a beam is then ``LASERSCAN_BEAM[coord]`` in the pose's frame."""
    out = convert_poses(poses, coord)
    if coord == "ros":
        out[..., 3:7] = quaternion_multiply(out[..., 3:7], _ROS_SCAN_PLANE)
    return out


def quaternion_multiply(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Hamilton product a ⊗ b of (..., 4) xyzw quaternions."""
    a, b = np.asarray(a, dtype=np.float64), np.asarray(b, dtype=np.float64)
    ax, ay, az, aw = np.moveaxis(a, -1, 0)
    bx, by, bz, bw = np.moveaxis(b, -1, 0)
    return np.stack(
        [
            aw * bx + ax * bw + ay * bz - az * by,
            aw * by - ax * bz + ay * bw + az * bx,
            aw * bz + ax * by - ay * bx + az * bw,
            aw * bw - ax * bx - ay * by - az * bz,
        ],
        axis=-1,
    )


def rotate(quaternions: np.ndarray, vectors: np.ndarray) -> np.ndarray:
    """Rotate (..., 3) vectors by (..., 4) xyzw quaternions."""
    q = np.asarray(quaternions, dtype=np.float64)
    v = np.asarray(vectors, dtype=np.float64)
    u, w = q[..., :3], q[..., 3:4]
    t = 2.0 * np.cross(u, v)
    return v + w * t + np.cross(u, t)


def coord_info(coord: str) -> dict:
    _check(coord)
    return {
        "source": "lh_yup",
        **_INFO[coord],
        "units": "meters",
        "quaternion_order": "xyzw",
    }
