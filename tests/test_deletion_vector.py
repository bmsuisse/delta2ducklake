import json
from pathlib import Path

import pytest

from delta2ducklake.delta.actions import parse_action
from delta2ducklake.delta.deletion_vector import (
    deleted_row_positions,
    parse_deletion_vector_bitmap,
    z85_decode,
)
from delta2ducklake.storage import LocalStorageBackend

FIXTURES = Path(__file__).parent / "fixtures" / "delta-rs" / "table-with-dv-small"
STORAGE = LocalStorageBackend()


def test_z85_decode_matches_rfc32_test_vector():
    # The official ZeroMQ RFC 32 test case: 8 bytes <-> "HelloWorld".
    assert z85_decode("HelloWorld") == bytes([0x86, 0x4F, 0xD2, 0x6F, 0xB5, 0x59, 0xF7, 0x5B])


def test_z85_decode_real_dv_uuid_matches_filename():
    # "vBn[lx{q8@P<9BNH/isA" is the real pathOrInlineDv from table-with-dv-small's add action;
    # decoding it must reconstruct the UUID in the actual .bin file's name on disk.
    decoded = z85_decode("vBn[lx{q8@P<9BNH/isA")
    assert decoded.hex() == "61d16c75699446b7a15b8b538852e50e"


def test_z85_decode_rejects_bad_length():
    with pytest.raises(ValueError, match="multiple of 5"):
        z85_decode("abc")


def test_parse_deletion_vector_bitmap_rejects_bad_magic():
    with pytest.raises(ValueError, match="magic number"):
        parse_deletion_vector_bitmap(b"\x00\x00\x00\x00")


def test_deleted_row_positions_real_fixture():
    """table-with-dv-small's commit 1 deletes rows via `DELETE ... WHERE value IN (0, 9)` on a
    10-row table where row i has value i (confirmed from the fixture's own add.stats and
    commitInfo.operationParameters) -- so positions {0, 9} is the one and only correct answer,
    not just "some 2-element set".
    """
    commit_path = FIXTURES / "_delta_log" / "00000000000000000001.json"
    lines = [json.loads(line) for line in commit_path.read_text().splitlines() if line.strip()]
    add = next(parse_action(line) for line in lines if "add" in line)
    assert add.deletion_vector is not None
    assert add.deletion_vector.cardinality == 2

    positions = deleted_row_positions(add.deletion_vector, STORAGE, str(FIXTURES))
    assert positions == {0, 9}
