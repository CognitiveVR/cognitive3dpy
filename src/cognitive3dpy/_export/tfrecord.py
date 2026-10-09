"""TFRecord files of ``tf.train.Example`` protos, without TensorFlow.

A record is ``uint64 length | masked crc32c(length) | data | masked crc32c(data)``,
little-endian. An Example is a map of feature name to a bytes, float32 or int64
list; it is encoded here straight to protobuf wire format.
"""

from __future__ import annotations

import struct
from collections.abc import Iterator
from pathlib import Path

import google_crc32c
import numpy as np

MAX_RECORD_BYTES = 2**31 - 1  # protobuf's message size limit


def _masked_crc(data: bytes) -> int:
    crc = google_crc32c.value(data)
    return (((crc >> 15) | (crc << 17)) + 0xA282EAD8) & 0xFFFFFFFF


def _varint(value: int) -> bytes:
    value &= 0xFFFFFFFFFFFFFFFF
    out = bytearray()
    while value > 0x7F:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)
    return bytes(out)


def _field(number: int, payload: bytes) -> bytes:
    """A length-delimited field."""
    return _varint(number << 3 | 2) + _varint(len(payload)) + payload


def bytes_feature(values) -> bytes:
    return _field(1, b"".join(_field(1, v) for v in values))


def float_feature(values) -> bytes:
    data = np.ascontiguousarray(values, dtype="<f4").reshape(-1).tobytes()
    return _field(2, _field(1, data))


def int64_feature(values) -> bytes:
    flat = np.asarray(values, dtype=np.int64).reshape(-1)
    return _field(3, _field(1, b"".join(_varint(int(v)) for v in flat)))


def example(features: dict[str, bytes]) -> bytes:
    """``features`` maps a name to the output of one of the ``*_feature`` helpers."""
    entries = b"".join(
        _field(1, _field(1, name.encode()) + _field(2, feature))
        for name, feature in features.items()
    )
    return _field(1, entries)


class TFRecordWriter:
    def __init__(self, path: Path):
        self._file = open(path, "wb")
        self.bytes = 0

    def write(self, record: bytes) -> None:
        if len(record) > MAX_RECORD_BYTES:
            raise ValueError(
                f"A record of {len(record)} bytes is over TensorFlow's 2 GiB limit."
            )
        header = struct.pack("<Q", len(record))
        self._file.write(header + struct.pack("<I", _masked_crc(header)))
        self._file.write(record + struct.pack("<I", _masked_crc(record)))
        self.bytes += len(header) + len(record) + 8

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> TFRecordWriter:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def read_records(path: Path) -> Iterator[bytes]:
    """Records of a TFRecord file, checking both checksums."""
    with open(path, "rb") as f:
        while header := f.read(12):
            if len(header) < 12:
                raise ValueError(f"{path}: truncated record header")
            (length,) = struct.unpack("<Q", header[:8])
            if struct.unpack("<I", header[8:])[0] != _masked_crc(header[:8]):
                raise ValueError(f"{path}: bad length checksum")
            data = f.read(length)
            footer = f.read(4)
            if len(data) < length or len(footer) < 4:
                raise ValueError(f"{path}: truncated record")
            if struct.unpack("<I", footer)[0] != _masked_crc(data):
                raise ValueError(f"{path}: bad data checksum")
            yield data


def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = shift = 0
    while True:
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, pos
        shift += 7


def _fields(data: bytes) -> Iterator[tuple[int, bytes]]:
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        if tag & 7 != 2:
            raise ValueError(f"unexpected wire type {tag & 7}")
        length, pos = _read_varint(data, pos)
        yield tag >> 3, data[pos : pos + length]
        pos += length


def parse_example(data: bytes) -> dict[str, list | np.ndarray]:
    """Feature name to a list of bytes, a float32 array or an int64 array."""
    out: dict[str, list | np.ndarray] = {}
    for _, features in _fields(data):
        for _, entry in _fields(features):
            parts = dict(_fields(entry))
            name = parts[1].decode()
            kind, payload = next(_fields(parts.get(2, b"")), (1, b""))
            values = b"".join(v for _, v in _fields(payload))
            if kind == 1:
                out[name] = [v for _, v in _fields(payload)]
            elif kind == 2:
                out[name] = np.frombuffer(values, dtype="<f4")
            else:
                ints, pos = [], 0
                while pos < len(values):
                    value, pos = _read_varint(values, pos)
                    ints.append(value - (1 << 64) if value >> 63 else value)
                out[name] = np.array(ints, dtype=np.int64)
    return out
