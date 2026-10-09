"""Tests for _export/mapping.py, coords.py and timeline.py (no lerobot needed)."""

import numpy as np
import pytest
from lerobot_fakes import make_session

from cognitive3dpy._export.coords import (
    LASERSCAN_BEAM,
    convert_poses,
    convert_positions,
    convert_quaternions,
    convert_scan_poses,
    coord_info,
    quaternion_multiply,
    rotate,
)
from cognitive3dpy._export.errors import C3DDatasetError
from cognitive3dpy._export.mapping import (
    EVENT_KEY,
    GAZE_KEY,
    SENSORS_KEY,
    STATE_KEY,
    TIME_KEY,
    MappingOptions,
    SessionCaches,
    build_episode,
    build_layout,
    camera_feature_key,
    camera_pose_key,
)
from cognitive3dpy._export.sessions import parse_session
from cognitive3dpy._export.timeline import (
    derive_fps,
    event_index_per_frame,
    hold_indices,
    parse_clock,
    sample_and_hold,
)


class MemoryCache:
    def __init__(self, blobs):
        self.blobs = blobs

    def read(self, ref):
        return self.blobs[(ref.version_id, ref.stream_id, ref.date, ref.seq)]


def memory_caches(fake):
    return SessionCaches(
        camera=MemoryCache(fake.frames), lidar=MemoryCache(fake.lidar_blobs)
    )


def session_and_cache(**kwargs):
    fake = make_session(**kwargs)
    docs = {name.lower(): doc for name, doc in fake.docs.items()}
    session = parse_session(fake.project_id, fake.session_id, docs)
    return fake, session, memory_caches(fake)


def plan_for(options=None, **session_kwargs):
    fake, session, caches = session_and_cache(**session_kwargs)
    options = options or MappingOptions()
    layout = build_layout([session], options, lambda _s: caches)
    plan = build_episode(session, layout, options, lambda _s: caches)
    return fake, session, layout, plan


# coords


def quat_rotate(q, v):
    x, y, z, w = q
    u = np.array([x, y, z])
    return 2 * np.dot(u, v) * u + (w * w - np.dot(u, u)) * v + 2 * w * np.cross(u, v)


def unit_quaternions(n, seed=1):
    q = np.random.default_rng(seed).normal(size=(n, 4))
    return q / np.linalg.norm(q, axis=1, keepdims=True)


def test_rotate_matches_the_quaternion_sandwich():
    q = unit_quaternions(50)
    v = np.random.default_rng(2).normal(size=(50, 3))
    for qi, vi, ri in zip(q, v, rotate(q, v)):
        assert np.allclose(ri, quat_rotate(qi, vi), atol=1e-12)


def test_quaternion_multiply_composes_rotations():
    a, b = unit_quaternions(20, 3), unit_quaternions(20, 4)
    v = np.random.default_rng(5).normal(size=(20, 3))
    assert np.allclose(
        rotate(quaternion_multiply(a, b), v), rotate(a, rotate(b, v)), atol=1e-12
    )


def test_mirror_is_an_involution_and_commutes_with_rotation():
    q = unit_quaternions(200)
    v = np.random.default_rng(1).normal(size=(200, 3))
    assert np.allclose(convert_positions(convert_positions(v, "rh_yup"), "rh_yup"), v)
    assert np.allclose(
        convert_quaternions(convert_quaternions(q, "rh_yup"), "rh_yup"), q
    )
    left = rotate(convert_quaternions(q, "rh_yup"), convert_positions(v, "rh_yup"))
    assert np.allclose(left, convert_positions(rotate(q, v), "rh_yup"), atol=1e-12)


def test_mirror_flips_handedness():
    mx, my, mz = convert_positions(np.eye(3), "rh_yup")
    assert np.allclose(np.cross(mx, my), -mz)


def test_matches_the_replay_viewer_convert_pose():
    # cvr-session-replay convertPose: position [-x, y, z], rotation [qx, -qy, -qz, qw]
    pose = np.array([1.0, 2.0, 3.0, 0.1, 0.2, 0.3, 0.9])
    expected = [-1.0, 2.0, 3.0, 0.1, -0.2, -0.3, 0.9]
    assert np.allclose(convert_poses(pose, "rh_yup"), expected)


@pytest.mark.parametrize("coord", ["rh_yup", "ros", "native"])
def test_every_conversion_commutes_with_rotation(coord):
    q = unit_quaternions(200, 7)
    v = np.random.default_rng(8).normal(size=(200, 3))
    converted = convert_quaternions(q, coord)
    assert np.allclose(np.linalg.norm(converted, axis=1), 1.0)
    left = rotate(converted, convert_positions(v, coord))
    assert np.allclose(left, convert_positions(rotate(q, v), coord), atol=1e-12)


def test_ros_axes_are_forward_left_up():
    # recorded: x right, y up, z forward
    right, up, forward = np.eye(3)
    assert np.allclose(convert_positions(forward, "ros"), [1, 0, 0])
    assert np.allclose(convert_positions(-right, "ros"), [0, 1, 0])
    assert np.allclose(convert_positions(up, "ros"), [0, 0, 1])
    x, y, z = convert_positions(np.eye(3), "ros")
    assert np.allclose(np.cross(x, y), -z)  # left-handed in, right-handed out


def test_a_yaw_about_up_stays_a_yaw_about_up():
    half = np.sqrt(0.5)
    yaw_90 = np.array([0.0, half, 0.0, half])  # recorded: 90 degrees about +y
    ros = convert_quaternions(yaw_90, "ros")
    assert np.allclose(ros[:2], 0.0) and abs(abs(ros[2]) - half) < 1e-12


@pytest.mark.parametrize("coord", ["rh_yup", "ros", "native"])
def test_scan_poses_follow_the_documented_beam(coord):
    q = unit_quaternions(50, 9)
    p = np.random.default_rng(10).normal(size=(50, 3))
    theta = np.linspace(-3, 3, 50)
    r = np.linspace(0.2, 9, 50)
    local = np.stack([r * np.cos(theta), r * np.sin(theta), np.zeros(50)], axis=1)
    world = convert_positions(rotate(q, local) + p, coord)

    pose = convert_scan_poses(np.concatenate([p, q], axis=1), coord)
    beam = {
        "(r cos θ, r sin θ, 0)": local,
        "(-r cos θ, r sin θ, 0)": local * [-1, 1, 1],
    }[LASERSCAN_BEAM[coord]]
    assert np.allclose(rotate(pose[:, 3:], beam) + pose[:, :3], world, atol=1e-12)


def test_ros_poses_of_a_turtlebot4_match_its_urdf():
    # tb4 sim session, t = 604 s: base pose and rplidar scan pose as recorded
    head = np.array([-4.0017, 0.0, -13.4531, 0.0, -0.71528, 0.0, 0.69884])
    scan = np.array(
        [-3.95703, 0.192915, -13.45207, 0.505758, 0.494175, -0.494175, 0.505758]
    )
    base = convert_poses(head, "ros")
    lidar = convert_scan_poses(scan, "ros")
    assert abs(lidar[2] - 0.193) < 1e-3  # rplidar_link sits 0.193 m up
    base_x, base_y = rotate(base[3:], [1, 0, 0]), rotate(base[3:], [0, 1, 0])
    assert np.allclose(rotate(lidar[3:], [0, 0, 1]), [0, 0, 1], atol=1e-3)
    # rplidar_link is yawed +90 degrees on the base: its x is the base's y
    assert np.allclose(rotate(lidar[3:], [1, 0, 0]), base_y, atol=0.05)
    assert np.allclose(rotate(lidar[3:], [0, 1, 0]), -base_x, atol=0.05)
    # and mounted 4 cm behind base_link
    offset = rotate(base[3:] * [-1, -1, -1, 1], lidar[:3] - base[:3])
    assert np.allclose(offset, [-0.04, 0.0, 0.193], atol=0.01)


def test_coord_info():
    assert coord_info("rh_yup")["mirror"] == "x"
    assert coord_info("native")["mirror"] is None
    assert coord_info("ros")["up"] == "+z"
    with pytest.raises(ValueError):
        coord_info("zup")


# timeline


def test_hold_and_sample_and_hold():
    src = np.array([1.0, 3.0])
    assert hold_indices(src, np.array([0.5, 1.0, 2.0, 9.0])).tolist() == [-1, 0, 0, 1]
    values = np.array([[10.0], [30.0]])
    query = np.array([0.0, 1.0, 2.0, 3.5])
    nan = sample_and_hold(src, values, query)
    assert np.isnan(nan[0, 0]) and nan[1:, 0].tolist() == [10.0, 10.0, 30.0]
    clamp = sample_and_hold(src, values, query, before_first="clamp")
    assert clamp[0, 0] == 10.0
    assert np.isnan(sample_and_hold(np.array([]), np.zeros(0), query)).all()
    with pytest.raises(ValueError):
        sample_and_hold(src, values, query, before_first="zero")


def test_event_index_per_frame_windows_and_latest_wins():
    frames = np.array([1.0, 2.0, 3.0, 4.0])
    events = np.array([0.0, 1.5, 1.9, 3.0, 9.0])
    codes = np.array([7, 1, 2, 3, 4])
    assert event_index_per_frame(events, codes, frames).tolist() == [7, 2, 3, -1]


def test_derive_fps():
    assert derive_fps(np.arange(0, 10, 0.1)) == 10
    assert derive_fps(np.array([0.0, 0.6, 1.2])) == 2
    assert derive_fps(np.array([5.0])) == 10


@pytest.mark.parametrize(
    "clock,expected",
    [
        (None, ("auto", None)),
        ("gaze", ("gaze", None)),
        ("camera", ("camera", None)),
        ("camera:front", ("camera", "front")),
        ("fixed:15", ("fixed", 15.0)),
    ],
)
def test_parse_clock(clock, expected):
    assert parse_clock(clock) == expected


@pytest.mark.parametrize("clock", ["fixed:0", "fixed:abc", "wallclock", "camera:"])
def test_parse_clock_rejects(clock):
    with pytest.raises(ValueError):
        parse_clock(clock)


# mapping


def test_default_layout_is_camera_paced_with_every_stream():
    fake, session, layout, plan = plan_for()
    assert layout.clock == ("camera", "front") and layout.fps == 2
    assert layout.cameras == {"front": (12, 16)}
    assert set(layout.features) == {
        STATE_KEY,
        GAZE_KEY,
        SENSORS_KEY,
        EVENT_KEY,
        camera_feature_key("front"),
        camera_pose_key("front"),
        TIME_KEY,
    }
    assert len(plan) == 20
    rows = fake.docs["CAMERA"]["cameraFrames"]["42"]
    assert plan.numeric[TIME_KEY][:, 0].tolist() == [r["date"] for r in rows]
    assert [r.seq for r in plan.images[camera_feature_key("front")]] == list(range(20))


def test_values_are_mirrored_and_sample_held():
    fake, session, layout, plan = plan_for()
    cam = fake.docs["CAMERA"]["cameraFrames"]["42"]
    gaze = fake.docs["GAZE"]["data"]["42"]
    gaze_t = np.array([g["time"] for g in gaze])
    for i in (0, 7, 19):
        j = max(int(np.searchsorted(gaze_t, cam[i]["time"], side="right")) - 1, 0)
        p = gaze[j]["p"]
        assert np.allclose(plan.numeric[STATE_KEY][i], [-p[0], p[1], p[2]], atol=1e-5)
        pose = cam[i]["pose"]
        expected = [-pose[0], pose[1], pose[2], pose[3], -pose[4], -pose[5], pose[6]]
        assert np.allclose(plan.numeric[camera_pose_key("front")][i], expected)
    battery = plan.numeric[SENSORS_KEY][:, layout.sensor_names.index("battery.percent")]
    times = np.array([r["time"] for r in cam])
    assert np.isnan(battery[times < 1.0]).all()
    assert np.all(battery[(times >= 1.0) & (times < 3.0)] == 99.0)
    assert np.all(battery[times >= 5.0] == 90.0)
    events = plan.numeric[EVENT_KEY][:, 0]
    hit = [layout.event_names[e] for e in events if e >= 0]
    assert hit == ["c3d.sessionStart", "pick.start", "pick.done"]


def test_native_coordinates_and_head_rotation_on_the_gaze_clock():
    options = MappingOptions(clock="gaze", coord="native", head_rotation=True)
    fake, session, layout, plan = plan_for(options)
    assert layout.fps == 10 and len(plan) == 100
    row = fake.docs["GAZE"]["data"]["42"][10]
    assert np.allclose(plan.numeric[STATE_KEY][10], row["p"] + row["r"], atol=1e-6)
    assert np.isnan(plan.numeric[GAZE_KEY][5]).all()  # row 5 looked at an object


def test_fixed_clock_resamples_on_an_exact_grid():
    _, _, layout, plan = plan_for(MappingOptions(clock="fixed:5"))
    assert layout.fps == 5
    assert np.allclose(np.diff(plan.numeric[TIME_KEY][:, 0]), 200, atol=1)


def test_stream_subset_and_sensor_filter():
    options = MappingOptions(streams={"head", "sensors"}, sensors=["odom.speed"])
    _, _, layout, _ = plan_for(options)
    assert set(layout.features) == {STATE_KEY, SENSORS_KEY, TIME_KEY}
    assert layout.features[SENSORS_KEY]["names"] == ["odom.speed"]


def test_invalid_requests():
    with pytest.raises(C3DDatasetError, match="Unknown sensor"):
        plan_for(MappingOptions(sensors=["nope"]))
    with pytest.raises(ValueError, match="Unknown stream"):
        plan_for(MappingOptions(streams={"radar"}))
    with pytest.raises(C3DDatasetError, match="no \\['camera'\\] data"):
        plan_for(MappingOptions(streams={"camera"}), cameras=())
    with pytest.raises(C3DDatasetError, match="not in every session"):
        plan_for(MappingOptions(clock="camera:rear"))
    with pytest.raises(C3DDatasetError, match="no \\['lidar'\\] data"):
        plan_for(MappingOptions(streams={"lidar"}))
