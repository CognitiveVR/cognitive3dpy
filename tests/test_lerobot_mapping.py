"""Tests for _lerobot/mapping.py, coords.py and timeline.py (no lerobot needed)."""

import numpy as np
import pytest
from lerobot_fakes import make_session

from cognitive3dpy._lerobot.coords import (
    coord_info,
    mirror_poses,
    mirror_positions,
    mirror_quaternions,
)
from cognitive3dpy._lerobot.errors import C3DDatasetError
from cognitive3dpy._lerobot.mapping import (
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
from cognitive3dpy._lerobot.sessions import parse_session
from cognitive3dpy._lerobot.timeline import (
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


def test_mirror_is_an_involution_and_commutes_with_rotation():
    rng = np.random.default_rng(1)
    q = rng.normal(size=(200, 4))
    q /= np.linalg.norm(q, axis=1, keepdims=True)
    v = rng.normal(size=(200, 3))
    assert np.allclose(mirror_positions(mirror_positions(v)), v)
    assert np.allclose(mirror_quaternions(mirror_quaternions(q)), q)
    for qi, vi in zip(q, v):
        left = quat_rotate(mirror_quaternions(qi), mirror_positions(vi))
        assert np.allclose(left, mirror_positions(quat_rotate(qi, vi)), atol=1e-12)


def test_mirror_flips_handedness():
    mx, my, mz = mirror_positions(np.eye(3))
    assert np.allclose(np.cross(mx, my), -mz)


def test_matches_the_replay_viewer_convert_pose():
    # cvr-session-replay convertPose: position [-x, y, z], rotation [qx, -qy, -qz, qw]
    pose = np.array([1.0, 2.0, 3.0, 0.1, 0.2, 0.3, 0.9])
    assert np.allclose(mirror_poses(pose), [-1.0, 2.0, 3.0, 0.1, -0.2, -0.3, 0.9])


def test_coord_info():
    assert coord_info("rh_yup")["mirror"] == "x"
    assert coord_info("native")["mirror"] is None
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
