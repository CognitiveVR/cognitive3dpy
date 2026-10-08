"""Tests for _lerobot/sessions.py."""

import numpy as np
import pytest
from lerobot_fakes import make_session

from cognitive3dpy._client import C3DNotFoundError, get_base_url
from cognitive3dpy._lerobot.http import HttpClient
from cognitive3dpy._lerobot.sessions import fetch_session, parse_session


def docs_of(fake):
    return {
        "gaze": fake.docs.get("GAZE"),
        "sensors": fake.docs.get("SENSORS"),
        "events": fake.docs.get("EVENTS"),
        "camera": fake.docs.get("CAMERA"),
    }


def test_parse_project_scoped_sections():
    fake = make_session()
    session = parse_session(fake.project_id, fake.session_id, docs_of(fake))
    assert session.start_timestamp == fake.start
    assert session.streams == {"head", "gaze", "sensors", "events", "camera"}
    assert len(session.gaze) == 100
    assert np.all(np.diff(session.gaze.times) > 0)
    # rows with an object id keep the head pose but drop the gaze point
    assert np.isnan(session.gaze.gaze_points[5]).all()
    assert not np.isnan(session.gaze.positions[5]).any()
    battery = session.sensors["battery.percent"]
    assert battery.times.tolist() == [1.0, 3.0, 5.0]
    assert battery.values.tolist() == [99.0, 95.0, 90.0]
    names = [e.name for e in session.events]
    assert names == ["c3d.sessionStart", "pick.start", "pick.done"]
    assert session.events[1].properties == {"item": "box"}
    refs = session.cameras["front"]
    assert len(refs) == 20 and refs[0].seq == 0 and refs[0].version_id == 42
    assert refs[0].date == int(round((fake.start + refs[0].time) * 1000))


def test_scene_scoped_list_shape_is_accepted():
    gaze = dict(make_session().docs["GAZE"])
    gaze["data"] = gaze["data"]["42"]
    session = parse_session(1, "s", {"gaze": gaze})
    assert len(session.gaze) == 100 and session.streams == {"head", "gaze"}


def test_rows_from_several_versions_are_merged_in_time_order():
    gaze = dict(make_session().docs["GAZE"])
    rows = gaze["data"]["42"]
    gaze["data"] = {"2": rows[50:], "1": rows[:50]}
    session = parse_session(1, "s", {"gaze": gaze})
    assert np.all(np.diff(session.gaze.times) > 0)


def test_missing_sections():
    fake = make_session(sensors=False, events=False, cameras=())
    assert parse_session(1, "s", docs_of(fake)).streams == {"head", "gaze"}


def test_fetch_session_reports_unknown_session(edge):
    with HttpClient("test-api-key", get_base_url()) as http:
        with pytest.raises(C3DNotFoundError, match="not found or has no data yet"):
            fetch_session(http, 1234, "no-such-session", progress=False)


def test_fetch_session_reads_every_section(edge):
    fake = edge.add(make_session())
    with HttpClient("test-api-key", get_base_url()) as http:
        session = fetch_session(http, fake.project_id, fake.session_id, progress=False)
    assert session.streams == {"head", "gaze", "sensors", "events", "camera"}
    assert edge.calls["json"] == 6  # lidar and boundary answer 404
