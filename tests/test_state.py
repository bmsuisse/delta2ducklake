from pathlib import Path

import pytest

from delta2ducklake.delta.state import UnsupportedTableFeatureError, load_table_state
from delta2ducklake.storage import LocalStorageBackend

FIXTURES = Path(__file__).parent / "fixtures"
STORAGE = LocalStorageBackend()


def _delta_io(name: str) -> str:
    return str(FIXTURES / "delta-io" / name)


def _delta_rs(name: str) -> str:
    return str(FIXTURES / "delta-rs" / name)


def test_load_table_state_basic_checkpoint_table():
    state = load_table_state(STORAGE, _delta_io("checkpoint"))
    assert state.version == 14
    assert set(state.active_files) == {"15"}
    assert state.metadata.schema_string
    assert state.protocol.min_reader_version == 1


def test_load_table_state_time_travel():
    table = _delta_io("delete-re-add-same-file-different-transactions")
    assert set(load_table_state(STORAGE, table, end_version=0).active_files) == {"foo"}
    assert load_table_state(STORAGE, table, end_version=1).active_files == {}
    assert set(load_table_state(STORAGE, table, end_version=2).active_files) == {"foo"}
    assert set(load_table_state(STORAGE, table, end_version=3).active_files) == {"foo", "bar"}


def test_column_mapping_table_raises_by_default():
    with pytest.raises(UnsupportedTableFeatureError):
        load_table_state(STORAGE, _delta_rs("table_with_column_mapping"))


def test_column_mapping_table_loads_when_allowed():
    state = load_table_state(
        STORAGE, _delta_rs("table_with_column_mapping"), allow_column_mapping=True
    )
    assert state.metadata.column_mapping_mode == "name"
    assert len(state.active_files) == 2


def test_deletion_vector_table_raises_by_default():
    with pytest.raises(UnsupportedTableFeatureError):
        load_table_state(STORAGE, _delta_rs("table-with-dv-small"))


def test_deletion_vector_table_loads_when_allowed():
    state = load_table_state(STORAGE, _delta_rs("table-with-dv-small"), allow_deletion_vectors=True)
    assert len(state.active_files) == 1
    add = next(iter(state.active_files.values()))
    assert add.deletion_vector is not None
    assert add.deletion_vector.cardinality == 2


def test_table_with_columnmapping_mode_id_raises_by_default():
    with pytest.raises(UnsupportedTableFeatureError):
        load_table_state(STORAGE, _delta_io("table-with-columnmapping-mode-id"))


def test_dv_with_columnmapping_requires_both_flags():
    table = _delta_io("dv-with-columnmapping")
    with pytest.raises(UnsupportedTableFeatureError):
        load_table_state(STORAGE, table)
    with pytest.raises(UnsupportedTableFeatureError):
        load_table_state(STORAGE, table, allow_column_mapping=True)
    with pytest.raises(UnsupportedTableFeatureError):
        load_table_state(STORAGE, table, allow_deletion_vectors=True)
    state = load_table_state(
        STORAGE, table, allow_column_mapping=True, allow_deletion_vectors=True
    )
    assert state.metadata.column_mapping_mode != "none"
