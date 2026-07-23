import json
from pathlib import Path

import pytest

from delta2ducklake.delta.schema import (
    ArrayType,
    MapType,
    PrimitiveType,
    StructType,
    ducklake_primitive_type,
    parse_schema_string,
)

FIXTURES = Path(__file__).parent / "fixtures"


def _schema_string_from_commit(path: Path) -> str:
    for line in path.read_text().splitlines():
        obj = json.loads(line)
        if "metaData" in obj:
            return obj["metaData"]["schemaString"]
    raise AssertionError(f"no metaData action found in {path}")


def test_parse_flat_primitive_schema():
    schema = parse_schema_string(
        json.dumps(
            {
                "type": "struct",
                "fields": [
                    {"name": "id", "type": "long", "nullable": False, "metadata": {}},
                    {"name": "name", "type": "string", "nullable": True, "metadata": {}},
                ],
            }
        )
    )
    assert isinstance(schema, StructType)
    assert [f.name for f in schema.fields] == ["id", "name"]
    assert schema.fields[0].nullable is False
    assert schema.fields[0].type == PrimitiveType("long")
    assert schema.fields[1].type == PrimitiveType("string")


def test_parse_nested_struct_schema_real_fixture():
    schema_string = _schema_string_from_commit(
        FIXTURES / "delta-io" / "data-reader-nested-struct" / "_delta_log"
        / "00000000000000000000.json"
    )
    schema = parse_schema_string(schema_string)
    a_field = next(f for f in schema.fields if f.name == "a")
    assert isinstance(a_field.type, StructType)
    ac_field = next(f for f in a_field.type.fields if f.name == "ac")
    assert isinstance(ac_field.type, StructType)
    assert [f.name for f in ac_field.type.fields] == ["aca", "acb"]
    assert ac_field.type.fields[0].type == PrimitiveType("integer")
    assert ac_field.type.fields[1].type == PrimitiveType("long")


def test_parse_map_schema_real_fixture():
    schema_string = _schema_string_from_commit(
        FIXTURES / "delta-io" / "data-reader-map" / "_delta_log" / "00000000000000000000.json"
    )
    schema = parse_schema_string(schema_string)
    e_field = next(f for f in schema.fields if f.name == "e")
    assert isinstance(e_field.type, MapType)
    assert e_field.type.key_type == PrimitiveType("string")
    assert e_field.type.value_type == PrimitiveType("decimal(1,0)")

    # 'f' is a map whose value is an array of a one-field struct: nested map -> array -> struct
    f_field = next(f for f in schema.fields if f.name == "f")
    assert isinstance(f_field.type, MapType)
    assert isinstance(f_field.type.value_type, ArrayType)
    assert isinstance(f_field.type.value_type.element_type, StructType)


def test_parse_deeply_nested_array_schema_real_fixture():
    schema_string = _schema_string_from_commit(
        FIXTURES / "delta-io" / "data-reader-array-complex-objects" / "_delta_log"
        / "00000000000000000000.json"
    )
    schema = parse_schema_string(schema_string)
    three_d = next(f for f in schema.fields if f.name == "3d_int_list")
    assert isinstance(three_d.type, ArrayType)
    assert isinstance(three_d.type.element_type, ArrayType)
    assert isinstance(three_d.type.element_type.element_type, ArrayType)
    assert three_d.type.element_type.element_type.element_type == PrimitiveType("integer")


def test_parse_column_mapping_physical_names_real_fixture():
    schema_string = _schema_string_from_commit(
        FIXTURES / "delta-rs" / "table_with_column_mapping" / "_delta_log"
        / "00000000000000000000.json"
    )
    schema = parse_schema_string(schema_string)
    company = next(f for f in schema.fields if f.name == "Company Very Short")
    assert company.physical_name == "col-173b4db9-b5ad-427f-9e75-516aae37fbbb"
    assert company.column_mapping_id == 1


@pytest.mark.parametrize(
    ("delta_type", "ducklake_type"),
    [
        ("string", "varchar"),
        ("long", "int64"),
        ("integer", "int32"),
        ("short", "int16"),
        ("byte", "int8"),
        ("float", "float32"),
        ("double", "float64"),
        ("boolean", "boolean"),
        ("binary", "blob"),
        ("date", "date"),
        ("timestamp", "timestamptz"),
        ("timestamp_ntz", "timestamp"),
        ("decimal(10,2)", "decimal(10,2)"),
        ("decimal(10, 2)", "decimal(10,2)"),
    ],
)
def test_ducklake_primitive_type_mapping(delta_type, ducklake_type):
    assert ducklake_primitive_type(delta_type) == ducklake_type


def test_ducklake_primitive_type_rejects_unsupported():
    with pytest.raises(ValueError):
        ducklake_primitive_type("void")
