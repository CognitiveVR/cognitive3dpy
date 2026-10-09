"""Tests for lidar and boundary: decoding, parsing, download, mapping, sidecar."""

import json

import numpy as np
import pytest
import zstandard
from lerobot_fakes import (
    LASER_RANGES,
    cloud_points,
    laserscan_ranges,
    make_session,
    pointcloud_blob,
)

from cognitive3dpy._client import C3DNotFoundError, get_base_url
from cognitive3dpy._export.camera import MediaCache, StreamUnavailable
from cognitive3dpy._export.http import HttpClient
from cognitive3dpy._export.lidar import (
    LidarDecodeError,
    LidarDownloader,
    decode_laserscan,
    decode_pointcloud,
)
from cognitive3dpy._export.mapping import (
    BOUNDARY_POSE_KEY,
    BOUNDARY_SHAPE_KEY,
    MappingOptions,
    SessionCaches,
    build_episode,
    build_layout,
    lidar_key,
)
from cognitive3dpy._export.models import LidarScanRef
from cognitive3dpy._export.sessions import fetch_session, parse_session
from cognitive3dpy._export.sidecar import write_session_sidecar


def laserscan_ref(count=4, intensities=False):
    return LidarScanRef(
        version_id=1,
        sensor_id="scan",
        kind="laserscan",
        date=100,
        seq=0,
        time=0.0,
        frame_id=None,
        pose=(0.0,) * 7,
        range_count=count,
        has_intensities=intensities,
    )


def cloud_ref(count, *, position="int16_mm", color="rgb565", intensity="none", zstd=1):
    return LidarScanRef(
        version_id=1,
        sensor_id="depth",
        kind="pointcloud",
        date=100,
        seq=0,
        time=0.0,
        frame_id=None,
        pose=(0.0,) * 7,
        point_count=count,
        position_format=position,
        color_format=color,
        intensity_format=intensity,
        zstd_level=zstd,
    )


def parsed(**kwargs):
    fake = make_session(**kwargs)
    docs = {name.lower(): doc for name, doc in fake.docs.items()}
    return fake, parse_session(fake.project_id, fake.session_id, docs)


class MemoryCache:
    def __init__(self, blobs):
        self.blobs = blobs

    def read(self, ref):
        return self.blobs[(ref.version_id, ref.stream_id, ref.date, ref.seq)]


def caches_of(fake):
    return SessionCaches(
        camera=MemoryCache(fake.frames), lidar=MemoryCache(fake.lidar_blobs)
    )


# decoding


def test_decode_laserscan_with_and_without_intensities():
    ranges = np.array([1.0, np.inf, 2.5, np.nan], dtype="<f4")
    got, intensities = decode_laserscan(ranges.tobytes(), laserscan_ref())
    assert np.array_equal(got, ranges, equal_nan=True) and intensities is None

    blob = ranges.tobytes() + np.arange(4, dtype="<f4").tobytes()
    got, intensities = decode_laserscan(blob, laserscan_ref(intensities=True))
    assert intensities.tolist() == [0.0, 1.0, 2.0, 3.0]


@pytest.mark.parametrize("size", [0, 12, 20])
def test_decode_laserscan_rejects_a_blob_of_the_wrong_length(size):
    with pytest.raises(LidarDecodeError, match="expected 16"):
        decode_laserscan(b"\0" * size, laserscan_ref())


def test_decode_zstd_int16_mm_rgb565_cloud():
    positions, _, _, rgb = cloud_points(3)
    cloud = decode_pointcloud(pointcloud_blob(3), cloud_ref(len(positions)))
    assert cloud.positions.dtype == np.float32 and cloud.positions.shape == (8, 3)
    assert np.allclose(cloud.positions, positions)
    assert np.array_equal(cloud.colors, rgb) and cloud.intensities is None


def test_decode_float32_rgb888_uint8_intensity_uncompressed():
    points = np.array([[0.5, -1.0, 2.0], [3.25, 0.0, -0.75]], dtype=np.float32)
    colors = np.array([[1, 2, 3], [250, 251, 252]], dtype=np.uint8)
    blob = points.T.astype("<f4").tobytes() + colors.tobytes() + bytes([7, 9])
    ref = cloud_ref(2, position="float32", color="rgb888", intensity="uint8", zstd=0)
    cloud = decode_pointcloud(blob, ref)
    assert np.array_equal(cloud.positions, points)
    assert np.array_equal(cloud.colors, colors)
    assert cloud.intensities.tolist() == [7, 9]


def test_decode_rgb332():
    blob = np.zeros((3, 2), dtype="<i2").tobytes() + bytes([0b11100000, 0b00000011])
    cloud = decode_pointcloud(blob, cloud_ref(2, color="rgb332", zstd=0))
    assert cloud.colors.tolist() == [[255, 0, 0], [0, 0, 255]]


def test_decode_pointcloud_errors():
    with pytest.raises(LidarDecodeError, match="expected"):
        decode_pointcloud(b"\0" * 10, cloud_ref(8, zstd=0))
    with pytest.raises(LidarDecodeError, match="zstd"):
        decode_pointcloud(b"not zstd", cloud_ref(8))
    with pytest.raises(LidarDecodeError, match="unsupported format"):
        decode_pointcloud(b"", cloud_ref(8, position="int8"))
    small = zstandard.ZstdCompressor().compress(b"\0" * 10)
    with pytest.raises(LidarDecodeError):
        decode_pointcloud(small, cloud_ref(8))


# parsing


def test_parse_lidar_and_boundary():
    _, session = parsed(lidar=True, boundary=True)
    assert set(session.lidar) == {"scan", "depth"}
    assert "lidar" in session.streams and "boundary" in session.streams
    scans = session.lidar["scan"]
    assert len(scans) == 30 and scans[0].kind == "laserscan"
    assert scans[0].range_count == LASER_RANGES and scans[0].range_max == 10.0
    assert all(a.time < b.time for a, b in zip(scans, scans[1:]))
    clouds = session.lidar["depth"]
    assert clouds[2].point_count == 7 and clouds[2].zstd_level == 1
    assert clouds[0].color_format == "rgb565"
    boundary = session.boundary
    assert boundary.times.tolist() == [2.0, 4.0, 6.0]
    assert boundary.shape_times.tolist() == [1.5, 5.0]  # sorted
    assert boundary.shapes[0][0].tolist() == [-1.0, 0.0, -1.0]


def test_sessions_without_lidar_or_boundary():
    _, session = parsed()
    assert session.lidar == {} and session.boundary is None
    assert "lidar" not in session.streams and "boundary" not in session.streams


def test_fetch_session_reads_lidar_and_boundary_sections(edge):
    fake = edge.add(make_session(lidar=True, boundary=True))
    with HttpClient("test-api-key", get_base_url()) as http:
        session = fetch_session(http, fake.project_id, fake.session_id, progress=False)
    assert {"lidar", "boundary"} <= session.streams
    assert edge.calls["json"] == 6


# download


@pytest.fixture
def lidar_setup(edge, tmp_path):
    fake = edge.add(make_session(lidar=True))
    http = HttpClient("test-api-key", get_base_url(), sleep=lambda _s: None)
    session = fetch_session(
        http, fake.project_id, fake.session_id, ["gaze", "lidar"], progress=False
    )
    cache = MediaCache(tmp_path / "c", http.host, 1234, fake.session_id, kind="lidar")

    def download(**kwargs):
        downloader = LidarDownloader(http, progress=False, **kwargs)
        return downloader.download(cache, 1234, fake.session_id, session.lidar)

    yield fake, session, cache, download
    http.close()


def assert_all_cached(fake, session, cache):
    for refs in session.lidar.values():
        for ref in refs:
            key = (ref.version_id, ref.sensor_id, ref.date, ref.seq)
            assert cache.read(ref) == fake.lidar_blobs[key]


def test_lidar_download_streams_every_scan_and_reuses_the_cache(lidar_setup, edge):
    fake, session, cache, download = lidar_setup
    stats = download()
    assert_all_cached(fake, session, cache)
    assert stats.scans == len(fake.lidar_blobs) == stats.via_stream
    assert stats.transports == {"stream"}
    assert (
        edge.calls["lidar_stream"] == 2 and edge.calls["lidar"] == 0
    )  # a page per sensor
    again = download()
    assert again.from_cache == stats.scans and again.transports == {"cache"}
    assert edge.calls["lidar_stream"] == 2


def test_lidar_stream_pages_through_the_cursor(lidar_setup, edge):
    fake, session, cache, download = lidar_setup
    stats = download(page_limit=7)
    assert_all_cached(fake, session, cache)
    assert stats.via_stream == len(fake.lidar_blobs)
    assert edge.calls["lidar_stream"] == 5 + 2  # 30 laserscans / 7, 9 clouds / 7


def test_truncated_lidar_pages_resume_from_the_last_whole_scan(lidar_setup, edge):
    fake, session, cache, download = lidar_setup
    edge.truncate_lidar_streams = 2
    stats = download(page_limit=10)
    assert_all_cached(fake, session, cache)
    assert stats.via_stream == len(fake.lidar_blobs) and stats.via_per_scan == 0


def test_a_partly_cached_sensor_streams_only_the_tail(lidar_setup):
    fake, session, cache, download = lidar_setup
    for ref in session.lidar["scan"][:20]:
        cache.write(ref, fake.lidar_blobs[(ref.version_id, "scan", ref.date, ref.seq)])
    stats = download()
    assert stats.from_cache == 20 and stats.via_stream == len(fake.lidar_blobs) - 20
    assert_all_cached(fake, session, cache)


def test_falls_back_to_one_request_per_scan_without_the_stream(lidar_setup, edge):
    fake, session, cache, download = lidar_setup
    edge.lidar_stream_available = False
    stats = download()
    assert_all_cached(fake, session, cache)
    assert stats.transports == {"per-scan"}
    assert stats.via_per_scan == len(fake.lidar_blobs)
    assert edge.calls["lidar_stream"] == 1  # probed once, then remembered


def test_stream_only_transport_raises_without_the_stream(lidar_setup, edge):
    edge.lidar_stream_available = False
    with pytest.raises(StreamUnavailable):
        lidar_setup[3](transport="stream")


def test_per_scan_transport_never_touches_the_stream(lidar_setup, edge):
    stats = lidar_setup[3](transport="per-scan")
    assert stats.transports == {"per-scan"} and edge.calls["lidar_stream"] == 0


def test_lidar_download_raises_on_a_scan_the_api_cannot_serve(lidar_setup):
    fake, session, cache, download = lidar_setup
    victim = session.lidar["scan"][4]
    del fake.lidar_blobs[(victim.version_id, "scan", victim.date, victim.seq)]
    with pytest.raises(C3DNotFoundError):  # missing from the stream, 404 per scan
        download()


def test_unknown_lidar_transport():
    with pytest.raises(ValueError, match="lidar_transport"):
        LidarDownloader(None, transport="carrier-pigeon")


# mapping


def plan_for(options=None, **session_kwargs):
    fake, session = parsed(**session_kwargs)
    caches = caches_of(fake)
    options = options or MappingOptions()
    layout = build_layout([session], options, lambda _s: caches)
    plan = build_episode(session, layout, options, lambda _s: caches)
    return fake, session, layout, plan


def test_lidar_and_boundary_features():
    _, _, layout, plan = plan_for(lidar=True, boundary=True)
    assert "lidar" in layout.streams and "boundary" in layout.streams
    for key, shape in [
        (lidar_key("scan", "ranges"), (LASER_RANGES,)),
        (lidar_key("scan", "pose"), (7,)),
        (lidar_key("depth", "scan_index"), (1,)),
        (lidar_key("depth", "pose"), (7,)),
        (BOUNDARY_POSE_KEY, (7,)),
        (BOUNDARY_SHAPE_KEY, (1,)),
    ]:
        assert tuple(layout.features[key]["shape"]) == shape
        assert plan.numeric[key].shape == (len(plan),) + shape
    assert lidar_key("scan", "intensities") not in layout.features


def test_laserscan_ranges_are_held_and_cleaned():
    fake, session, _, plan = plan_for(lidar=True)
    ranges = plan.numeric[lidar_key("scan", "ranges")]
    scan_times = np.array([r.time for r in session.lidar["scan"]])
    for i, t in enumerate(plan.times):
        held = int(np.searchsorted(scan_times, t, side="right")) - 1
        if held < 0:
            assert np.isnan(ranges[i]).all()
            continue
        expected = laserscan_ranges(session.lidar["scan"][held].seq)
        expected[[2, 5]] = np.nan  # no return, beyond range_max
        assert np.array_equal(ranges[i], expected, equal_nan=True)
    pose = plan.numeric[lidar_key("scan", "pose")]
    held = int(np.searchsorted(scan_times, plan.times[-1], side="right")) - 1
    p = session.lidar["scan"][held].pose
    expected_pose = [-p[0], p[1], p[2], p[3], -p[4], -p[5], p[6]]
    assert np.allclose(pose[-1], expected_pose)


def test_pointcloud_scan_index_and_boundary_hold():
    _, session, _, plan = plan_for(lidar=True, boundary=True)
    index = plan.numeric[lidar_key("depth", "scan_index")][:, 0]
    cloud_times = np.array([r.time for r in session.lidar["depth"]])
    expected = np.searchsorted(cloud_times, plan.times, side="right") - 1
    assert index.tolist() == expected.tolist() and index[0] == -1
    assert np.isnan(plan.numeric[lidar_key("depth", "pose")][index == -1]).all()

    pose = plan.numeric[BOUNDARY_POSE_KEY]
    shape = plan.numeric[BOUNDARY_SHAPE_KEY][:, 0]
    early = plan.times < 2.0
    assert np.isnan(pose[early]).all()
    late = plan.times >= 6.0
    assert np.allclose(pose[late], [-1.0, 0.0, -1.0, 0.0, -0.38268343, 0.0, 0.92387953])
    assert (shape[plan.times < 1.5] == -1).all()
    assert (shape[(plan.times >= 1.5) & (plan.times < 5.0)] == 0).all()
    assert (shape[plan.times >= 5.0] == 1).all()


def test_streams_can_leave_lidar_and_boundary_out():
    options = MappingOptions(streams={"head", "camera"})
    _, _, layout, _ = plan_for(options, lidar=True, boundary=True)
    assert not layout.lidar and not layout.boundary


def test_a_corrupt_laserscan_becomes_a_nan_row_with_a_warning():
    fake, session = parsed(lidar=True)
    first = session.lidar["scan"][0]
    fake.lidar_blobs[(first.version_id, "scan", first.date, first.seq)] = b"\0" * 3
    caches = caches_of(fake)
    options = MappingOptions(clock="gaze")
    layout = build_layout([session], options, lambda _s: caches)
    with pytest.warns(UserWarning, match="Skipping lidar scan"):
        plan = build_episode(session, layout, options, lambda _s: caches)
    assert plan.skipped_scans == {"scan": 1}


# sidecar


def test_sidecar_files(tmp_path):
    fake, session, layout, plan = plan_for(lidar=True, boundary=True)
    summary = write_session_sidecar(
        tmp_path, plan, layout, caches_of(fake), "rh_yup", progress=False
    )
    assert summary["scan"]["scans"] == 30 and summary["depth"]["skipped"] == 0
    assert summary["boundary"] == {"poses": 3, "shapes": 2}

    scans = np.load(tmp_path / "lidar" / "scan" / "scans.npz")
    assert scans["ranges"].shape == (30, LASER_RANGES)
    expected = laserscan_ranges(4)
    expected[[2, 5]] = np.nan
    assert np.array_equal(scans["ranges"][4], expected, equal_nan=True)
    assert float(scans["range_max"]) == 10.0

    index = json.loads((tmp_path / "lidar" / "depth" / "index.json").read_text())
    assert index["coordinate_system"]["target"] == "rh_yup"
    entry = index["scans"][3]
    cloud = np.load(tmp_path / "lidar" / "depth" / entry["file"])
    positions, _, _, rgb = cloud_points(3)
    mirrored = positions * np.array([-1, 1, 1], dtype=np.float32)
    assert np.allclose(cloud["points"], mirrored)
    assert np.array_equal(cloud["colors"], rgb) and entry["points"] == len(positions)

    boundary = json.loads((tmp_path / "boundary.json").read_text())
    assert [s["time"] for s in boundary["shapes"]] == [1.5, 5.0]
    assert boundary["shapes"][0]["points"][0] == [1.0, 0.0, -1.0]  # x mirrored
    events = json.loads((tmp_path / "events.json").read_text())
    assert events["events"][1]["point"] == [-1.0, 0.0, 0.0]


def test_sidecar_native_coordinates_are_untouched(tmp_path):
    options = MappingOptions(coord="native")
    fake, _, layout, plan = plan_for(options, lidar=True, boundary=True)
    write_session_sidecar(
        tmp_path, plan, layout, caches_of(fake), "native", progress=False
    )
    cloud = np.load(tmp_path / "lidar" / "depth" / "000001.npz")
    assert np.allclose(cloud["points"], cloud_points(1)[0])
    boundary = json.loads((tmp_path / "boundary.json").read_text())
    assert boundary["shapes"][0]["points"][0] == [-1.0, 0.0, -1.0]
