"""ROS 2 message definitions (Jazzy common_interfaces, comments stripped), in the
``ros2msg`` schema form MCAP and rosbag2 embed: the type, then each dependency
under an ``MSG: pkg/Type`` header."""

from __future__ import annotations

_SEPARATOR = "=" * 80

# type -> (definition, dependencies)
_DEFS: dict[str, tuple[str, tuple[str, ...]]] = {
    "builtin_interfaces/Time": ("int32 sec\nuint32 nanosec", ()),
    "std_msgs/Header": (
        "builtin_interfaces/Time stamp\nstring frame_id",
        ("builtin_interfaces/Time",),
    ),
    "std_msgs/Float64": ("float64 data", ()),
    "std_msgs/String": ("string data", ()),
    "geometry_msgs/Vector3": ("float64 x\nfloat64 y\nfloat64 z", ()),
    "geometry_msgs/Point": ("float64 x\nfloat64 y\nfloat64 z", ()),
    "geometry_msgs/Point32": ("float32 x\nfloat32 y\nfloat32 z", ()),
    "geometry_msgs/Quaternion": (
        "float64 x 0\nfloat64 y 0\nfloat64 z 0\nfloat64 w 1",
        (),
    ),
    "geometry_msgs/Pose": (
        "geometry_msgs/Point position\ngeometry_msgs/Quaternion orientation",
        ("geometry_msgs/Point", "geometry_msgs/Quaternion"),
    ),
    "geometry_msgs/PoseStamped": (
        "std_msgs/Header header\ngeometry_msgs/Pose pose",
        ("std_msgs/Header", "geometry_msgs/Pose"),
    ),
    "geometry_msgs/PointStamped": (
        "std_msgs/Header header\ngeometry_msgs/Point point",
        ("std_msgs/Header", "geometry_msgs/Point"),
    ),
    "geometry_msgs/Polygon": (
        "geometry_msgs/Point32[] points",
        ("geometry_msgs/Point32",),
    ),
    "geometry_msgs/PolygonStamped": (
        "std_msgs/Header header\ngeometry_msgs/Polygon polygon",
        ("std_msgs/Header", "geometry_msgs/Polygon"),
    ),
    "geometry_msgs/Transform": (
        "geometry_msgs/Vector3 translation\ngeometry_msgs/Quaternion rotation",
        ("geometry_msgs/Vector3", "geometry_msgs/Quaternion"),
    ),
    "geometry_msgs/TransformStamped": (
        "std_msgs/Header header\nstring child_frame_id\n"
        "geometry_msgs/Transform transform",
        ("std_msgs/Header", "geometry_msgs/Transform"),
    ),
    "tf2_msgs/TFMessage": (
        "geometry_msgs/TransformStamped[] transforms",
        ("geometry_msgs/TransformStamped",),
    ),
    "sensor_msgs/CompressedImage": (
        "std_msgs/Header header\nstring format\nuint8[] data",
        ("std_msgs/Header",),
    ),
    "sensor_msgs/LaserScan": (
        "std_msgs/Header header\nfloat32 angle_min\nfloat32 angle_max\n"
        "float32 angle_increment\nfloat32 time_increment\nfloat32 scan_time\n"
        "float32 range_min\nfloat32 range_max\nfloat32[] ranges\n"
        "float32[] intensities",
        ("std_msgs/Header",),
    ),
    "sensor_msgs/PointField": (
        "uint8 INT8 = 1\nuint8 UINT8 = 2\nuint8 INT16 = 3\nuint8 UINT16 = 4\n"
        "uint8 INT32 = 5\nuint8 UINT32 = 6\nuint8 FLOAT32 = 7\nuint8 FLOAT64 = 8\n"
        "string name\nuint32 offset\nuint8 datatype\nuint32 count",
        (),
    ),
    "sensor_msgs/PointCloud2": (
        "std_msgs/Header header\nuint32 height\nuint32 width\n"
        "sensor_msgs/PointField[] fields\nbool is_bigendian\nuint32 point_step\n"
        "uint32 row_step\nuint8[] data\nbool is_dense",
        ("std_msgs/Header", "sensor_msgs/PointField"),
    ),
}


def schema_name(msg_type: str) -> str:
    """``sensor_msgs/LaserScan`` -> ``sensor_msgs/msg/LaserScan`` (rosbag2 naming)."""
    package, name = msg_type.split("/")
    return f"{package}/msg/{name}"


def definition(msg_type: str) -> str:
    """The type's definition followed by every type it depends on."""
    order: list[str] = []

    def visit(name: str) -> None:
        for dependency in _DEFS[name][1]:
            if dependency not in order:
                order.append(dependency)
                visit(dependency)

    visit(msg_type)
    parts = [_DEFS[msg_type][0]]
    parts += [f"{_SEPARATOR}\nMSG: {name}\n{_DEFS[name][0]}" for name in order]
    return "\n".join(parts) + "\n"
