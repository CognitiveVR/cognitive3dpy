"""Auto-generated Polars type mappings from slicer_fields.yaml.

DO NOT EDIT MANUALLY. Regenerate with:
    uv run python scripts/sync_schema.py

Generated: 2026-04-09T23:45:39Z
Source: slicer_fields.yaml
"""

from __future__ import annotations

import polars as pl

# =============================================================================
# SESSION FIELDS
# Top-level fields on session documents (original API names).
# =============================================================================

SESSION_FIELD_TYPES: dict[str, pl.DataType] = {
    "date": pl.Utf8,
    "duration": pl.Int64,
    "participantId": pl.Utf8,
    "sessionId": pl.Utf8,
    "projectId": pl.Int64,
    "organizationId": pl.Int64,
    "userSessionNumber": pl.Int64,
    "sceneId": pl.Utf8,
    "versionId": pl.Int64,
    "hmdYawHistogram10degrees": pl.Int64,
    "hmdVirtualYawHistogram10degrees": pl.Int64,
    "hasDynamics": pl.Boolean,
    "hasFixations": pl.Boolean,
    "hasGazes": pl.Boolean,
    "hasSensors": pl.Boolean,
    "hasEvents": pl.Boolean,
    "gazeInterval": pl.Float64,
}


# =============================================================================
# SESSION PROPERTIES
# Nested in "properties" struct; names are dot-case originals.
# =============================================================================

SESSION_PROPERTY_TYPES: dict[str, pl.DataType] = {
    "c3d.app.name": pl.Utf8,
    "c3d.app.version": pl.Utf8,
    "c3d.app.engine": pl.Utf8,
    "c3d.app.engine.version": pl.Utf8,
    "c3d.app.sdktype": pl.Utf8,
    "c3d.version": pl.Utf8,
    "c3d.app.xrplugin": pl.Utf8,
    "c3d.app.oculus.appid": pl.Utf8,
    "c3d.app.plugin.version": pl.Utf8,
    "c3d.app.androidPlugin.version": pl.Utf8,
    "c3d.app.androidPlugin.hostName": pl.Utf8,
    "c3d.app.androidPlugin.networkHostName": pl.Utf8,
    "c3d.app.multiplayer.lobbyId": pl.Utf8,
    "c3d.device.hmd.type": pl.Utf8,
    "c3d.device.hmd.manufacturer": pl.Utf8,
    "c3d.device.eyetracking.type": pl.Utf8,
    "c3d.geo.country": pl.Utf8,
    "c3d.geo.countryIso": pl.Utf8,
    "c3d.geo.city": pl.Utf8,
    "c3d.geo.subdivision": pl.Utf8,
    "c3d.geo.subdivisionIso": pl.Utf8,
    "c3d.geo.qualifiedSubdivisionIso": pl.Utf8,
    "c3d.device.type": pl.Utf8,
    "c3d.device.manufacturer": pl.Utf8,
    "c3d.device.model": pl.Utf8,
    "c3d.device.os": pl.Utf8,
    "c3d.device.cpu": pl.Utf8,
    "c3d.device.cpu.vendor": pl.Utf8,
    "c3d.device.gpu": pl.Utf8,
    "c3d.device.gpu.vendor": pl.Utf8,
    "c3d.device.vendor": pl.Utf8,
    "c3d.device.serialnumber": pl.Utf8,
    "c3d.device.serial_number": pl.Utf8,
    "c3d.device.screenresolution": pl.Utf8,
    "c3d.deviceid": pl.Utf8,
    "c3d.oculusId": pl.Utf8,
    "c3d.username": pl.Utf8,
    "c3d.sessionname": pl.Utf8,
    "c3d.roomsizeDescription": pl.Utf8,
    "c3d.roomsizeDescriptionMeters": pl.Utf8,
    "c3d.participant.id": pl.Utf8,
    "c3d.participant.name": pl.Utf8,
    "c3d.participant.height": pl.Float64,
    "c3d.participant.armlength": pl.Float64,
    "c3d.participant.Age": pl.Float64,
    "c3d.participant.Sex": pl.Utf8,
    "c3d.participant.Color": pl.Utf8,
    "c3d.participant.Job": pl.Utf8,
    "c3d.multiplayer.lobbyId": pl.Utf8,
    "c3d.multiplayer.photonAppId": pl.Utf8,
    "c3d.multiplayer.photonPlayerId": pl.Utf8,
    "c3d.multiplayer.photonUserId": pl.Utf8,
    "c3d.multiplayer.photonServerAddress": pl.Utf8,
    "c3d.multiplayer.photonRoomName": pl.Utf8,
    "c3d.multiplayer.photonGameMode": pl.Utf8,
    "c3d.multiplayer.port": pl.Int64,
    "cvr.vr.display.family": pl.Utf8,
    "cvr.vr.display.model": pl.Utf8,
    "cvr.vr.enabled": pl.Utf8,
    "cvr.device.platform": pl.Utf8,
    "cvr.device.graphics.version": pl.Utf8,
    "cvr.device.graphics.memory": pl.Utf8,
    "c3d.geo.latitude": pl.Float64,
    "c3d.geo.longitude": pl.Float64,
    "c3d.device.memory": pl.Float64,
    "c3d.height": pl.Float64,
    "c3d.roomsize": pl.Float64,
    "c3d.multiplayer.maxNumberConnections": pl.Float64,
    "c3d.metrics.app_performance": pl.Float64,
    "c3d.metrics.average_fps": pl.Float64,
    "c3d.metrics.fps_score": pl.Float64,
    "c3d.metrics.battery_efficiency": pl.Float64,
    "c3d.metrics.immersion_score": pl.Float64,
    "c3d.metrics.presence_score": pl.Float64,
    "c3d.metrics.comfort_score": pl.Float64,
    "c3d.metrics.orientation_score": pl.Float64,
    "c3d.metrics.boundary_score": pl.Float64,
    "c3d.metrics.standing_percentage": pl.Float64,
    "c3d.metrics.controller_events_score": pl.Float64,
    "c3d.metrics.controller_engagement_score": pl.Float64,
    "c3d.metrics.dynamic_engagement_score": pl.Float64,
    "c3d.metrics.spatial_coverage_score": pl.Float64,
    "c3d.metrics.head_orientation_score": pl.Float64,
    "c3d.metrics.controller_ergonomic_score": pl.Float64,
    "c3d.metrics.ergonomics_score": pl.Float64,
    "c3d.metrics.cyberwellness_score": pl.Float64,
    "c3d.metrics.boundary_relative_yaw": pl.Float64,
    "c3d.metrics.virtual_yaw": pl.Float64,
    "c3d.metric_components.battery_drain_sum": pl.Float64,
    "c3d.metric_components.battery_drain_time_millis": pl.Float64,
    "c3d.metric_components.fps_data_point_sum": pl.Float64,
    "c3d.metric_components.fps_data_point_count": pl.Float64,
    "c3d.metric_components.average_controller_movement_meters_per_second": pl.Float64,
    "c3d.metric_components.fps_score.consistency_app_performance": pl.Float64,
    "c3d.metric_components.fps_score.degree_app_performance": pl.Float64,
    "c3d.metric_components.fps_score.fluctuation_app_performance": pl.Float64,
    "c3d.metric_components.fps_score.session_percentage": pl.Float64,
    "c3d.metric_components.presence_score.controller_movement_score": pl.Float64,
    "c3d.metric_components.presence_score.gaze_exploration_score": pl.Float64,
    "c3d.metric_components.presence_score.spatial_coverage_score": pl.Float64,
    "c3d.metric_components.presence_score.interruption_score": pl.Float64,
    "c3d.metric_components.comfort_score.head_orientation_score": pl.Float64,
    "c3d.metric_components.comfort_score.head_orientation_score_pitch_score":
        pl.Float64,
    "c3d.metric_components.comfort_score.head_orientation_score_roll_score": pl.Float64,
    "c3d.metric_components.comfort_score.controller_ergonomic_score": pl.Float64,
    "c3d.metric_components.comfort_score.controller_ergonomic_score_forward_reach_score":
        pl.Float64,
    "c3d.metric_components.comfort_score.controller_ergonomic_score_horizontal_reach_score":
        pl.Float64,
    "c3d.metric_components.comfort_score.controller_ergonomic_score_vertical_reach_score":
        pl.Float64,
    "c3d.metric_components.pitch_score": pl.Float64,
    "c3d.metric_components.roll_score": pl.Float64,
    "c3d.metric_components.forward_reach_score": pl.Float64,
    "c3d.metric_components.horizontal_reach_score": pl.Float64,
    "c3d.metric_components.vertical_reach_score": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_forwards_total": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_forwards_far": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_forwards_medium": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_forwards_near": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_horizontal_total": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_horizontal_far": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_horizontal_medium": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_horizontal_near": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_vertical_total": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_vertical_far": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_vertical_medium": pl.Float64,
    "c3d.metric_components.controller_ergo_counts_vertical_near": pl.Float64,
    "c3d.metric_components.dynamic_input_controller_percentage": pl.Float64,
    "c3d.metric_components.dynamic_input_hand_percentage": pl.Float64,
    "c3d.metric_components.dynamic_input_none_percentage": pl.Float64,
    "c3d.metric_components.cyberwellness.continuous_movement": pl.Float64,
    "c3d.metric_components.cyberwellness.acceleration_variability": pl.Float64,
    "c3d.metric_components.cyberwellness.translational_speed": pl.Float64,
    "c3d.metric_components.cyberwellness.translational_movement": pl.Float64,
    "c3d.metric_components.cyberwellness.visual_continuity": pl.Float64,
    "c3d.session_tag.test": pl.Boolean,
    "c3d.session_tag.junk": pl.Boolean,
    "c3d.app.inEditor": pl.Boolean,
    "c3d.device.eyetracking.enabled": pl.Boolean,
    "c3d.app.handtracking.enabled": pl.Boolean,
    "c3d.device.controllerinputs.enabled": pl.Boolean,
    "c3d.headphonespresent": pl.Boolean,
    "c3d.hmd.RunningOnBattery": pl.Boolean,
    "c3d.roomscale": pl.Boolean,
    "xrpf.allowed.location.data": pl.Boolean,
    "xrpf.allowed.hardware.data": pl.Boolean,
    "xrpf.allowed.bio.data": pl.Boolean,
    "xrpf.allowed.spatial.data": pl.Boolean,
    "xrpf.allowed.social.data": pl.Boolean,
}


# =============================================================================
# EVENT FIELDS
# Top-level fields on event documents (original API names).
# =============================================================================

EVENT_FIELD_TYPES: dict[str, pl.DataType] = {
    "eventName": pl.Utf8,
    "date": pl.Utf8,
    "objectId": pl.Utf8,
    "sessionRelativeDateTotal": pl.Utf8,
    "sessionRelativeDateGapless": pl.Utf8,
    "xCoord": pl.Float64,
    "yCoord": pl.Float64,
    "zCoord": pl.Float64,
}


# =============================================================================
# EVENT PROPERTIES
# Nested event properties (original names).
# =============================================================================

EVENT_PROPERTY_TYPES: dict[str, pl.DataType] = {
    "questionSetId": pl.Utf8,
    "hook": pl.Utf8,
}
