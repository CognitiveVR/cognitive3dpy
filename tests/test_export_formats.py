"""Tests for session_downloader's mcap, hdf5, rlds and json formats, end to end
against the fake API (none of them needs lerobot)."""

import importlib.util
import io
import json
from collections import defaultdict

import h5py
import httpx
import numpy as np
import pytest
from lerobot_fakes import (
    cloud_points,
    export_entry,
    jpeg,
    laserscan_ranges,
    make_session,
)
from PIL import Image

import cognitive3dpy as c3d
from cognitive3dpy import session_downloader
from cognitive3dpy._export import tfrecord
from cognitive3dpy._export.coords import convert_positions, rotate

SESSION = "1700000000_test-session"


@pytest.fixture
def fake(edge):
    return edge.add(make_session(SESSION, lidar=True, boundary=True))


def download(tmp_path, fake, **kwargs):
    kwargs.setdefault("output", tmp_path / "datasets")
    kwargs.setdefault("cache_dir", tmp_path / "cache")
    kwargs.setdefault("progress", False)
    session_id = kwargs.pop("session_id", fake.session_id)
    return session_downloader.download(session_id=session_id, **kwargs)


def head_ros(i):
    """The fake's i-th head position, (0.1 i, 1.5, -0.2 i), in REP-103 axes."""
    return [-0.2 * i, -0.1 * i, 1.5]


def decoded(data: bytes) -> np.ndarray:
    with Image.open(io.BytesIO(data)) as image:
        return np.asarray(image.convert("RGB"))


def manifest(path):
    return json.loads((path / "c3d" / "download.json").read_text())


# tfrecord: the bytes TensorFlow 2.21 writes for the same values


def test_example_encoding_matches_tensorflow():
    cases = {
        "steps/image": (
            tfrecord.bytes_feature([b"\xff\xd8jpeg", b""]),
            "0a1d0a1b0a0b73746570732f696d616765120c0a0a0a06ffd86a7065670a00",
        ),
        "state": (
            tfrecord.float_feature([0.5, -1.25, np.inf]),
            "0a1b0a190a0573746174651210120e0a0c0000003f0000a0bf0000807f",
        ),
        "event": (
            tfrecord.int64_feature([-1, 0, 300, 1700000000510]),
            "0a220a200a056576656e7412171a150a13ffffffffffffffffff0100ac02fed395ffbc31",
        ),
    }
    for name, (feature, expected) in cases.items():
        assert tfrecord.example({name: feature}).hex() == expected


def test_record_framing_matches_tensorflow(tmp_path):
    path = tmp_path / "x.tfrecord"
    with tfrecord.TFRecordWriter(path) as writer:
        writer.write(b"hello tfrecord")
        writer.write(b"")
    assert path.read_bytes().hex() == (
        "0e00000000000000c5e5693f68656c6c6f2074667265636f72649f5b7622"
        "000000000000000029039807d8ea82a2"
    )
    assert list(tfrecord.read_records(path)) == [b"hello tfrecord", b""]


def test_read_records_checks_the_checksums(tmp_path):
    path = tmp_path / "x.tfrecord"
    with tfrecord.TFRecordWriter(path) as writer:
        writer.write(b"payload")
    data = bytearray(path.read_bytes())
    data[14] ^= 0xFF
    path.write_bytes(bytes(data))
    with pytest.raises(ValueError, match="checksum"):
        list(tfrecord.read_records(path))


def test_parse_example_round_trip():
    record = tfrecord.example(
        {
            "b": tfrecord.bytes_feature([b"x", b"yz"]),
            "f": tfrecord.float_feature(np.array([[1.5, np.nan]], dtype=np.float32)),
            "i": tfrecord.int64_feature([-5, 2**40]),
        }
    )
    parsed = tfrecord.parse_example(record)
    assert parsed["b"] == [b"x", b"yz"]
    assert parsed["f"][0] == 1.5 and np.isnan(parsed["f"][1])
    assert parsed["i"].tolist() == [-5, 2**40]


# json


def test_json_is_the_dashboard_export_plus_camera_and_lidar(tmp_path, edge, fake):
    result = download(tmp_path, fake, format="json")
    root = tmp_path / "datasets" / "cognitive3d" / SESSION
    assert result.path == root and result.coord == "native"
    assert edge.calls["export"] == 1
    for json_type in ("GAZE", "EVENTS", "SENSORS", "BOUNDARY"):
        written = (root / f"{json_type.lower()}_{SESSION}.json").read_bytes()
        assert written == export_entry(fake, json_type)  # the zip's file, as-is
    gaze = json.loads((root / f"gaze_{SESSION}.json").read_bytes())
    assert gaze["data"][0]["time"] == pytest.approx(1_700_000_000.55)
    for json_type in ("CAMERA", "LIDAR"):  # not in the export: the replay API's
        served = httpx.Response(200, json=fake.docs[json_type]).content
        assert (root / f"{json_type.lower()}_{SESSION}.json").read_bytes() == served
    assert {p.name for p in result.files} == {
        f"{t.lower()}_{SESSION}.json" for t in fake.docs
    }
    for (version, camera, date, seq), data in fake.frames.items():
        path = root / "camera" / str(version) / camera / f"{date:013d}_{seq:010d}.jpg"
        assert path.read_bytes() == data
    for (version, sensor, date, seq), data in fake.lidar_blobs.items():
        path = root / "lidar" / str(version) / sensor / f"{date:013d}_{seq:010d}.bin"
        assert path.read_bytes() == data
    info = manifest(root)
    assert info["format"] == "json" and info["coord"]["target"] == "lh_yup"
    assert "laserscan" in info["blob_formats"] and "export" in info["conventions"]
    assert session_downloader.validate(root).ok


def test_json_without_camera_or_lidar_skips_the_replay_api(tmp_path, edge, fake):
    download(tmp_path, fake, format="json", streams={"gaze", "events"})
    assert edge.calls["export"] == 1 and edge.calls["json"] == 0


def test_json_of_a_missing_session(tmp_path, edge, fake):
    with pytest.raises(c3d.C3DNotFoundError, match="not found or has no data yet"):
        download(tmp_path, fake, session_id="nope", format="json")


def test_json_streams_pick_sections(tmp_path, fake):
    result = download(tmp_path, fake, format="json", streams={"head", "events"})
    names = sorted(p.name for p in result.files)
    assert names == [f"events_{SESSION}.json", f"gaze_{SESSION}.json"]
    assert not (result.path / "camera").exists()
    assert result.camera.frames == 0


def test_json_of_several_sessions_has_a_folder_each(tmp_path, edge, fake):
    other = edge.add(make_session("1700000100_other", start=1_700_000_100.0))
    result = download(
        tmp_path,
        fake,
        session_id=[fake.session_id, other.session_id],
        repo_id="test/both",
        format="json",
    )
    assert (result.path / SESSION / f"gaze_{SESSION}.json").exists()
    assert (result.path / "1700000100_other" / "camera").is_dir()
    assert session_downloader.validate(result.path).ok


def test_json_and_mcap_reject_another_coord(tmp_path, fake):
    with pytest.raises(ValueError, match="as recorded"):
        download(tmp_path, fake, format="json", coord="rh_yup")
    with pytest.raises(ValueError, match="REP-103"):
        download(tmp_path, fake, format="mcap", coord="native")


# hdf5


def test_robomimic_layout(tmp_path, fake):
    result = download(tmp_path, fake, format="hdf5")
    root = tmp_path / "datasets" / "cognitive3d" / SESSION
    assert result.files == [root / f"{SESSION}.hdf5"] and result.coord == "ros"
    with result.load() as f:
        demo = f["data/demo_0"]
        n = int(demo.attrs["num_samples"])
        assert n == 20 == f["data"].attrs["total"]
        env = json.loads(f["data"].attrs["env_args"])
        assert env["env_name"] == "cognitive3d" and env["type"] == 2
        assert [m.decode() for m in f["mask/train"][()]] == ["demo_0"]

        states = demo["states"][()]
        assert np.allclose(states[0], head_ros(0)) and np.allclose(
            states[1], head_ros(4)
        )
        assert np.allclose(demo["actions"][:-1], states[1:])
        assert np.allclose(demo["actions"][-1], states[-1])
        assert demo["dones"][()].tolist() == [0] * 19 + [1]
        assert np.all(demo["rewards"][()] == 0)

        obs = demo["obs"]
        assert set(obs) >= {
            "state",
            "gaze",
            "sensors",
            "event",
            "front_image",
            "front_pose",
            "lidar_scan_ranges",
            "lidar_scan_pose",
            "lidar_depth_scan_index",
            "boundary_pose",
            "c3d_time_ms",
        }
        assert list(obs["state"].attrs["names"]) == ["head.x", "head.y", "head.z"]
        images = obs["front_image"]
        assert images.shape == (20, 12, 16, 3) and images.dtype == np.uint8
        first_frame = next(iter(fake.frames.values()))
        assert np.array_equal(images[0], decoded(first_frame))
        assert obs["lidar_scan_ranges"].shape == (20, 8)
    info = manifest(root)
    assert info["format"] == "hdf5" and info["coord"]["target"] == "ros_rep103"
    assert info["hdf5"]["layout"] == "robomimic"
    assert (
        root / "c3d" / "sessions" / SESSION / "lidar" / "scan" / "scans.npz"
    ).exists()
    assert session_downloader.validate(root).ok


def test_aloha_layout(tmp_path, fake):
    result = download(tmp_path, fake, format="hdf5", hdf5_layout="aloha")
    assert [p.name for p in result.files] == ["episode_0.hdf5"]
    with h5py.File(result.files[0], "r") as f:
        assert not f.attrs["sim"]
        qpos = f["observations/qpos"][()]
        assert qpos.shape == (20, 3) and np.allclose(qpos[1], head_ros(4))
        times = np.round(np.arange(0.0, 10.0, 0.5) + 0.01, 3)
        expected = np.gradient(qpos.astype(np.float64), times, axis=0)
        assert np.allclose(f["observations/qvel"][()], expected, atol=1e-5)
        assert f["observations/images/front"].shape == (20, 12, 16, 3)
        assert np.allclose(f["action"][:-1], qpos[1:])
        assert "gaze" in f["observations"]
    assert session_downloader.validate(result.path).ok


def test_hdf5_of_several_sessions(tmp_path, edge, fake):
    other = edge.add(make_session("1700000100_other", start=1_700_000_100.0))
    ids = [fake.session_id, other.session_id]
    result = download(
        tmp_path, fake, session_id=ids, repo_id="test/both", format="hdf5"
    )
    with result.load() as f:
        assert sorted(f["data"]) == ["demo_0", "demo_1"]
        assert f["data/demo_1"].attrs["session_id"] == "1700000100_other"
    aloha = download(
        tmp_path,
        fake,
        session_id=ids,
        repo_id="test/both",
        format="hdf5",
        hdf5_layout="aloha",
    )
    assert [p.name for p in aloha.files] == ["episode_0.hdf5", "episode_1.hdf5"]


def test_hdf5_coord_can_be_overridden(tmp_path, fake):
    result = download(tmp_path, fake, format="hdf5", coord="rh_yup")
    with result.load() as f:
        assert np.allclose(f["data/demo_0/states"][1], [-0.4, 1.5, -0.8])


def test_unknown_hdf5_layout(tmp_path, fake):
    with pytest.raises(ValueError, match="hdf5_layout"):
        download(tmp_path, fake, format="hdf5", hdf5_layout="zarr")


# rlds


def rlds_episodes(path):
    shards = sorted(path.glob("*.tfrecord-*"))
    return [tfrecord.parse_example(r) for s in shards for r in tfrecord.read_records(s)]


def test_rlds_episode(tmp_path, fake):
    result = download(tmp_path, fake, format="rlds", task="walk the course")
    root = tmp_path / "datasets" / "cognitive3d" / SESSION
    assert [p.name for p in result.files] == [
        "cognitive3d_1700000000_test_session-train.tfrecord-00000-of-00001"
    ]
    info = json.loads((root / "dataset_info.json").read_text())
    assert info["name"] == "cognitive3d_1700000000_test_session"
    assert info["splits"][0]["shardLengths"] == ["1"]
    features = json.loads((root / "features.json").read_text())
    steps = features["featuresDict"]["features"]["steps"]["sequence"]["feature"]
    observation = steps["featuresDict"]["features"]["observation"]
    image = observation["featuresDict"]["features"]["image"]["image"]
    assert image["shape"]["dimensions"] == ["12", "16", "3"]
    assert image["encodingFormat"] == "jpeg"

    (episode,) = rlds_episodes(root)
    frames = [d for (_, _, _, _), d in sorted(fake.frames.items())]
    assert episode["steps/observation/image"] == frames  # recorded bytes, as-is
    state = episode["steps/observation/state"].reshape(-1, 3)
    assert len(state) == 20 and np.allclose(state[1], head_ros(4))
    action = episode["steps/action"].reshape(-1, 3)
    assert np.allclose(action[:-1], state[1:])
    assert episode["steps/is_first"].tolist() == [1] + [0] * 19
    assert episode["steps/is_last"].tolist() == [0] * 19 + [1]
    assert episode["steps/is_terminal"].tolist() == [0] * 19 + [1]
    assert np.all(episode["steps/discount"] == 1)
    assert episode["steps/language_instruction"] == [b"walk the course"] * 20
    assert episode["steps/observation/lidar_scan_ranges"].shape == (20 * 8,)
    assert episode["episode_metadata/session_id"] == [SESSION.encode()]
    assert episode["episode_metadata/start_time_ms"].tolist() == [1700000000500]
    assert manifest(root)["rlds"]["observation_names"]["observation.images.front"] == (
        "image"
    )
    assert session_downloader.validate(root).ok


def test_rlds_reencodes_a_frame_of_another_size(tmp_path, fake):
    key = sorted(fake.frames)[3]
    fake.frames[key] = jpeg(99, size=(8, 6))
    result = download(tmp_path, fake, format="rlds")
    (episode,) = rlds_episodes(result.path)
    assert decoded(episode["steps/observation/image"][3]).shape == (12, 16, 3)
    assert episode["steps/observation/image"][4] == fake.frames[sorted(fake.frames)[4]]


def test_rlds_of_several_sessions(tmp_path, edge, fake):
    other = edge.add(make_session("1700000100_other", start=1_700_000_100.0))
    result = download(
        tmp_path,
        fake,
        session_id=[fake.session_id, other.session_id],
        repo_id="test/both",
        format="rlds",
    )
    episodes = rlds_episodes(result.path)
    assert [e["episode_metadata/session_id"] for e in episodes] == [
        [SESSION.encode()],
        [b"1700000100_other"],
    ]


@pytest.mark.skipif(
    importlib.util.find_spec("tensorflow_datasets") is None,
    reason="needs tensorflow-datasets",
)
def test_rlds_loads_with_tfds(tmp_path, fake):  # pragma: no cover - optional
    import tensorflow_datasets as tfds

    result = download(tmp_path, fake, format="rlds")
    builder = tfds.builder_from_directory(str(result.path))
    (episode,) = list(builder.as_dataset(split="train"))
    steps = list(episode["steps"].as_numpy_iterator())
    assert len(steps) == 20
    assert steps[0]["observation"]["image"].shape == (12, 16, 3)


# mcap


def read_bag(path):
    from mcap.reader import make_reader
    from mcap_ros2.decoder import DecoderFactory

    out = defaultdict(list)
    with open(path, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for _, channel, message, msg in reader.iter_decoded_messages():
            out[channel.topic].append((message.log_time, msg))
    return out


def test_mcap_messages(tmp_path, fake):
    result = download(tmp_path, fake, format="mcap")
    root = tmp_path / "datasets" / "cognitive3d" / SESSION
    assert result.files == [root / f"{SESSION}_0.mcap"]
    assert (root / "metadata.yaml").exists()
    bag = read_bag(result.files[0])
    counts = {topic: len(msgs) for topic, msgs in bag.items()}
    assert counts == {
        "/c3d/session": 1,
        "/c3d/head/pose": 100,
        "/c3d/gaze": 90,  # every 10th sample looks at an object
        "/c3d/sensors/battery_percent": 3,
        "/c3d/sensors/odom_speed": 2,
        "/c3d/events": 3,
        "/c3d/camera/front/image/compressed": 20,
        "/c3d/lidar/scan/scan": 30,
        "/c3d/lidar/depth/points": 9,
        "/c3d/boundary": 2,
        "/tf": 100 + 20 + 30 + 9 + 3,
    }
    assert result.messages == sum(counts.values())

    stamp, pose = bag["/c3d/head/pose"][4]
    assert stamp == 1_700_000_000_950_000_000  # start 1_700_000_000.5 + 0.45 s
    p = pose.pose.position
    assert np.allclose([p.x, p.y, p.z], head_ros(4))
    assert pose.header.frame_id == "map"

    images = [m for _, m in bag["/c3d/camera/front/image/compressed"]]
    assert [m.data for m in images] == [d for _, d in sorted(fake.frames.items())]
    assert images[0].format == "jpeg"
    assert images[0].header.frame_id == "c3d/camera/front"

    _, scan = bag["/c3d/lidar/scan/scan"][2]
    ranges = np.array(scan.ranges, dtype=np.float32)
    assert np.array_equal(ranges, laserscan_ranges(2))  # inf and 20.0 as recorded
    assert scan.range_max == pytest.approx(10.0)
    assert scan.angle_max == pytest.approx(np.pi)

    _, cloud = bag["/c3d/lidar/depth/points"][1]
    fields = [(f.name, f.offset, f.datatype) for f in cloud.fields]
    assert fields == [("x", 0, 7), ("y", 4, 7), ("z", 8, 7), ("rgb", 12, 7)]
    data = np.frombuffer(
        bytes(cloud.data),
        dtype=[("x", "<f4"), ("y", "<f4"), ("z", "<f4"), ("rgb", "<u4")],
    )
    positions, _, _, rgb = cloud_points(1)
    points = np.stack([data["x"], data["y"], data["z"]], axis=1)
    assert np.allclose(points, convert_positions(positions, "ros"), atol=1e-6)
    rgb = rgb.astype(np.uint32)
    expected_rgb = (rgb[:, 0] << 16) | (rgb[:, 1] << 8) | rgb[:, 2]
    assert data["rgb"].tolist() == expected_rgb.tolist()

    events = [json.loads(m.data) for _, m in bag["/c3d/events"]]
    assert [e["name"] for e in events] == [
        "c3d.sessionStart",
        "pick.start",
        "pick.done",
    ]
    assert events[1]["point"] == [0.0, -1.0, 0.0] and events[1]["properties"]
    _, polygon = bag["/c3d/boundary"][0]
    assert polygon.header.frame_id == "c3d/boundary"
    assert len(polygon.polygon.points) == 4
    session_info = json.loads(bag["/c3d/session"][0][1].data)
    assert session_info["session_id"] == SESSION
    info = manifest(root)
    assert info["format"] == "mcap" and info["bags"][0]["messages"] == result.messages
    assert session_downloader.validate(root).ok


def test_mcap_laserscans_land_where_the_recording_puts_them(tmp_path, fake):
    result = download(tmp_path, fake, format="mcap")
    bag = read_bag(result.files[0])
    transforms = {
        (t, tf.transforms[0].child_frame_id): tf.transforms[0].transform
        for t, tf in bag["/tf"]
    }
    rows = fake.docs["LIDAR"]["lidarScans"]["42"]
    scans = [r for r in rows if r["kind"] == "laserscan"]
    for (stamp, scan), row in zip(bag["/c3d/lidar/scan/scan"], scans):
        transform = transforms[(stamp, "c3d/lidar/scan")]
        t, q = transform.translation, transform.rotation
        ranges = np.array(scan.ranges, dtype=np.float64)
        theta = scan.angle_min + np.arange(len(ranges)) * scan.angle_increment
        keep = np.isfinite(ranges)
        beam = np.stack([np.cos(theta), np.sin(theta), np.zeros_like(theta)], axis=1)
        local = beam[keep] * ranges[keep, None]
        got = rotate([q.x, q.y, q.z, q.w], local) + [t.x, t.y, t.z]
        # the recording: world = rotate(q, (r cos θ, r sin θ, 0)) + p, left-handed
        pose = np.array(row["pose"])
        expected = convert_positions(rotate(pose[3:], local) + pose[:3], "ros")
        assert np.allclose(got, expected, atol=1e-5)


def test_mcap_uses_the_recorded_ros_topics(tmp_path, edge):
    session = make_session(SESSION, lidar=True)
    session.docs["GAZE"]["properties"] = {
        "c3d.ros.camera.front.topic": "/oakd/rgb/preview/image_raw",
        "c3d.ros.lidar.scan.topic": "/scan",
        "c3d.ros.lidar.depth.topic": "not a topic",
        "c3d.ros.distro": "jazzy",
    }
    fake = edge.add(session)
    result = download(tmp_path, fake, format="mcap")
    topics = set(read_bag(result.files[0]))
    assert "/oakd/rgb/preview/image_raw/compressed" in topics
    assert "/scan" in topics and "/c3d/lidar/depth/points" in topics
    assert 'ros_distro: "jazzy"' in (result.path / "metadata.yaml").read_text()


def test_mcap_is_a_rosbag2_bag(tmp_path, fake):
    from rosbags.highlevel import AnyReader
    from rosbags.typesys import Stores, get_typestore

    result = download(tmp_path, fake, format="mcap")
    store = get_typestore(Stores.ROS2_JAZZY)
    with AnyReader([result.path], default_typestore=store) as reader:
        assert reader.message_count == result.messages
        for connection, _, raw in reader.messages():
            reader.deserialize(raw, connection.msgtype)


def test_mcap_of_several_sessions_has_a_bag_each(tmp_path, edge, fake):
    other = edge.add(make_session("1700000100_other", start=1_700_000_100.0))
    result = download(
        tmp_path,
        fake,
        session_id=[fake.session_id, other.session_id],
        repo_id="test/both",
        format="mcap",
    )
    assert [p.relative_to(result.path).as_posix() for p in result.files] == [
        f"{SESSION}/{SESSION}_0.mcap",
        "1700000100_other/1700000100_other_0.mcap",
    ]
    assert all((p.parent / "metadata.yaml").exists() for p in result.files)
    assert session_downloader.validate(result.path).ok


# output folders


def test_json_is_the_default_format(tmp_path, fake):
    result = download(tmp_path, fake)
    assert result.format == "json"
    assert (result.path / f"gaze_{SESSION}.json").exists()


def test_the_session_folder_first_then_a_format_suffix(tmp_path, fake):
    base = tmp_path / "datasets" / "cognitive3d" / SESSION
    assert download(tmp_path, fake).path == base
    assert download(tmp_path, fake, format="mcap").path == base.with_name(
        f"{SESSION}_mcap"
    )
    assert download(tmp_path, fake, format="hdf5").path == base.with_name(
        f"{SESSION}_hdf5"
    )
    (base / "notes.txt").write_text("mine")
    assert download(tmp_path, fake).path == base  # same format: updated in place
    assert (base / "notes.txt").read_text() == "mine"
    assert not base.with_name(f"{SESSION}_json").exists()
    assert download(tmp_path, fake, format="mcap").path == base.with_name(
        f"{SESSION}_mcap"
    )


def test_output_path_reads_the_format_of_an_earlier_download(tmp_path):
    from cognitive3dpy._export.download import output_path

    base = tmp_path / "s"
    assert output_path(base, "json") == base
    base.mkdir()
    assert output_path(base, "json") == tmp_path / "s_json"  # not a download
    (base / "c3d").mkdir()
    (base / "c3d" / "download.json").write_text('{"repo_id": "cognitive3d/s"}')
    assert output_path(base, "lerobot") == base  # from before formats existed
    assert output_path(base, "json") == tmp_path / "s_json"
    (base / "c3d" / "download.json").write_text('{"format": "rlds"}')
    assert output_path(base, "rlds") == base
    (base / "c3d" / "download.json").write_text("not json")
    assert output_path(base, "rlds") == tmp_path / "s_rlds"


def test_an_existing_suffixed_folder_is_written_over_where_files_conflict(
    tmp_path, fake
):
    base = tmp_path / "datasets" / "cognitive3d" / SESSION
    base.mkdir(parents=True)
    suffixed = base.with_name(f"{SESSION}_json")
    (suffixed / "camera").mkdir(parents=True)
    (suffixed / "notes.txt").write_text("mine")
    (suffixed / f"gaze_{SESSION}.json").write_text("stale")
    (suffixed / f"events_{SESSION}.json").mkdir()  # a folder where a file goes
    result = download(tmp_path, fake)
    assert result.path == suffixed
    assert (suffixed / "notes.txt").read_text() == "mine"
    assert (suffixed / f"gaze_{SESSION}.json").read_bytes() == export_entry(
        fake, "GAZE"
    )
    assert (suffixed / f"events_{SESSION}.json").is_file()
    assert len(list((suffixed / "camera").rglob("*.jpg"))) == 20
    assert not suffixed.with_name(f"{SESSION}_json.partial").exists()
    assert list(base.iterdir()) == []  # the session folder is not touched
    assert session_downloader.validate(suffixed).ok


def test_overwrite_false_only_refuses_an_existing_suffixed_folder(tmp_path, fake):
    base = tmp_path / "datasets" / "cognitive3d" / SESSION
    base.mkdir(parents=True)
    result = download(tmp_path, fake, overwrite=False)
    assert result.path == base.with_name(f"{SESSION}_json")
    with pytest.raises(FileExistsError, match="overwrite=True"):
        download(tmp_path, fake, overwrite=False)


def test_overwrite_false_refuses_the_same_format_in_the_session_folder(tmp_path, fake):
    download(tmp_path, fake)
    with pytest.raises(FileExistsError, match="overwrite=True"):
        download(tmp_path, fake, overwrite=False)


def test_merge_into_replaces_files_and_keeps_the_rest(tmp_path):
    from cognitive3dpy._export.sidecar import merge_into

    source, target = tmp_path / "new", tmp_path / "old"
    (source / "a").mkdir(parents=True)
    (source / "a" / "f.txt").write_text("new")
    (source / "b").write_text("file now")
    (target / "a").mkdir(parents=True)
    (target / "a" / "f.txt").write_text("old")
    (target / "a" / "keep.txt").write_text("keep")
    (target / "b").mkdir()
    (target / "b" / "x").write_text("was a folder")
    merge_into(source, target)
    assert (target / "a" / "f.txt").read_text() == "new"
    assert (target / "a" / "keep.txt").read_text() == "keep"
    assert (target / "b").read_text() == "file now"


# public API


def test_unknown_format(tmp_path, fake):
    with pytest.raises(ValueError, match="format must be one of"):
        download(tmp_path, fake, format="zarr")


@pytest.mark.parametrize(
    ("fmt", "module"),
    [("mcap", "mcap_ros2"), ("hdf5", "h5py"), ("rlds", "google_crc32c")],
)
def test_install_hint_names_the_format_extra(tmp_path, monkeypatch, fmt, module):
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *a: None if name == module else real(name, *a),
    )
    with pytest.raises(ImportError, match=rf"cognitive3dpy\[{fmt}\]"):
        session_downloader.download(session_id="s", output=tmp_path, format=fmt)
