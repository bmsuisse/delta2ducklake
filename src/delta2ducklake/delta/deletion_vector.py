"""Resolve and decode Delta deletion vectors into the set of deleted row positions.

Two binary formats, both from Delta's `PROTOCOL.md`, hand-decoded rather than reached for a
RoaringBitmap library dependency -- both formats are small, stable, fully specified, and (unlike
Parquet) not worth outsourcing:

1. **Z85** (RFC 32 / ZeroMQ): encodes the on-disk DV file's UUID within `pathOrInlineDv`, and
   *is* the entire `pathOrInlineDv` when `storageType == 'i'` (inline).
2. Delta's **deletion vector bitmap format**: a 4-byte magic number (little-endian) followed by a
   portable 64-bit `RoaringBitmap` (itself a sequence of 32-bit `RoaringBitmap`s, one per 32-bit
   "bucket" of the key space), per `PROTOCOL.md`'s Deletion Vector Format section and the
   RoaringBitmap project's own `RoaringFormatSpec`.

Every byte offset and byte order below was cross-checked against the real, Databricks-written
`table-with-dv-small` fixture (not just the prose spec): its `deletion_vector_<uuid>.bin` file
decodes, end to end, to exactly `{0, 9}` — matching that commit's own `DELETE ... WHERE value IN
(0, 9)` operation parameter recorded in the same commit's `commitInfo`.
"""

from __future__ import annotations

import struct
import uuid

from delta2ducklake.delta.actions import DeletionVectorDescriptor
from delta2ducklake.storage import StorageBackend, get_storage_backend

# --- Z85 (RFC 32) ---------------------------------------------------------------------------------

_Z85_ALPHABET = (
    "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"
)
_Z85_DECODE = {c: i for i, c in enumerate(_Z85_ALPHABET)}


def z85_decode(text: str) -> bytes:
    if len(text) % 5 != 0:
        raise ValueError(f"Z85 string length must be a multiple of 5, got {len(text)}")
    out = bytearray()
    for i in range(0, len(text), 5):
        value = 0
        for c in text[i : i + 5]:
            value = value * 85 + _Z85_DECODE[c]
        out += value.to_bytes(4, "big")
    return bytes(out)


# --- Delta deletion vector bitmap format ---------------------------------------------------------

_DV_MAGIC_NUMBER = 1681511377
_SERIAL_COOKIE_NO_RUNCONTAINER = 12346
_SERIAL_COOKIE = 12347
_NO_OFFSET_THRESHOLD = 4
_BITSET_CONTAINER_BYTES = 8192  # 1024 64-bit words = 65536 bits


def _parse_roaring32(data: bytes, start: int) -> tuple[list[int], int]:
    """Parse one 32-bit portable RoaringBitmap starting at `data[start:]`.

    Returns `(values, bytes_consumed)` -- `bytes_consumed` is needed by the 64-bit wrapper (below)
    since a bucket's 32-bit bitmap has no separate outer length prefix; its end is only knowable
    by fully parsing its cookie/descriptive/offset headers and containers.
    """
    pos = start
    (cookie,) = struct.unpack_from("<I", data, pos)
    if cookie == _SERIAL_COOKIE_NO_RUNCONTAINER:
        pos += 4
        (size,) = struct.unpack_from("<I", data, pos)
        pos += 4
        is_run = [False] * size
        has_offset_header = True
    elif (cookie & 0xFFFF) == _SERIAL_COOKIE:
        size = (cookie >> 16) + 1
        pos += 4
        run_bitmap_len = (size + 7) // 8
        run_bitmap = data[pos : pos + run_bitmap_len]
        pos += run_bitmap_len
        is_run = [bool(run_bitmap[i // 8] & (1 << (i % 8))) for i in range(size)]
        has_offset_header = size >= _NO_OFFSET_THRESHOLD
    else:
        raise ValueError(f"Bad RoaringBitmap cookie: {cookie}")

    keys: list[int] = []
    cardinalities: list[int] = []
    for _ in range(size):
        key, card_minus_1 = struct.unpack_from("<HH", data, pos)
        pos += 4
        keys.append(key)
        cardinalities.append(card_minus_1 + 1)

    if has_offset_header:
        pos += 4 * size  # container offsets, unused for sequential parsing

    values: list[int] = []
    for i in range(size):
        key, cardinality = keys[i], cardinalities[i]
        if is_run[i]:
            (run_count,) = struct.unpack_from("<H", data, pos)
            pos += 2
            for _ in range(run_count):
                run_start, run_len_minus_1 = struct.unpack_from("<HH", data, pos)
                pos += 4
                run_end = run_start + run_len_minus_1 + 1
                values.extend((key << 16) | v for v in range(run_start, run_end))
        elif cardinality <= 4096:
            for _ in range(cardinality):
                (v,) = struct.unpack_from("<H", data, pos)
                pos += 2
                values.append((key << 16) | v)
        else:
            container = data[pos : pos + _BITSET_CONTAINER_BYTES]
            pos += _BITSET_CONTAINER_BYTES
            for word_idx, word in enumerate(struct.unpack_from("<1024Q", container, 0)):
                w = word
                while w:
                    bit = w & (-w)
                    values.append((key << 16) | (word_idx * 64 + bit.bit_length() - 1))
                    w ^= bit
    return values, pos - start


def parse_deletion_vector_bitmap(data: bytes) -> set[int]:
    """Parse Delta's deletion-vector bitmap bytes (magic number + portable 64-bit RoaringBitmap)
    into the set of deleted row positions."""
    (magic,) = struct.unpack_from("<I", data, 0)
    if magic != _DV_MAGIC_NUMBER:
        raise ValueError(f"Bad deletion vector magic number: {magic} (expected {_DV_MAGIC_NUMBER})")
    pos = 4
    (num_buckets,) = struct.unpack_from("<Q", data, pos)
    pos += 8
    result: set[int] = set()
    for _ in range(num_buckets):
        (bucket_key,) = struct.unpack_from("<I", data, pos)
        pos += 4
        bucket_values, consumed = _parse_roaring32(data, pos)
        pos += consumed
        base = bucket_key << 32
        result.update(base | v for v in bucket_values)
    return result


# --- resolving a DeletionVectorDescriptor to its bitmap bytes -------------------------------------


def _resolve_on_disk_path(dv: DeletionVectorDescriptor) -> tuple[str, bool]:
    """Returns `(path, path_is_relative)` for `storageType in ('u', 'p')`."""
    if dv.storage_type == "p":
        return dv.path_or_inline_dv, False
    if dv.storage_type == "u":
        # <random prefix - optional><base85 encoded uuid>, the uuid always the last 20 chars.
        encoded = dv.path_or_inline_dv
        prefix, uuid_part = encoded[:-20], encoded[-20:]
        dv_uuid = uuid.UUID(bytes=z85_decode(uuid_part))
        filename = f"deletion_vector_{dv_uuid}.bin"
        return (f"{prefix}/{filename}" if prefix else filename), True
    raise ValueError(f"_resolve_on_disk_path() doesn't apply to storageType={dv.storage_type!r}")


def read_bitmap_data(
    dv: DeletionVectorDescriptor, storage: StorageBackend, table_root: str
) -> bytes:
    """Fetch the raw deletion-vector bitmap bytes (magic number onward) for a descriptor,
    handling all three `storageType`s.
    """
    if dv.storage_type == "i":
        return z85_decode(dv.path_or_inline_dv)

    rel_or_abs_path, path_is_relative = _resolve_on_disk_path(dv)
    if path_is_relative:
        full_path = storage.resolve(table_root, rel_or_abs_path)
        file_storage = storage
    else:
        full_path = rel_or_abs_path
        file_storage = get_storage_backend(full_path)

    offset = dv.offset or 0
    # `offset` points at the big-endian `dataSize` (4-byte) field that precedes the bitmap data in
    # the on-disk DV file storage format; `dv.size_in_bytes` already gives us that same length, so
    # we skip straight past it rather than re-reading and re-verifying it.
    return file_storage.read_range(full_path, offset + 4, dv.size_in_bytes)


def deleted_row_positions(
    dv: DeletionVectorDescriptor, storage: StorageBackend, table_root: str
) -> set[int]:
    return parse_deletion_vector_bitmap(read_bitmap_data(dv, storage, table_root))
