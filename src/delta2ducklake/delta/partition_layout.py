"""Detect Delta partition directories DuckDB's `ducklake` reader can't parse, and build the
Hive-style destination path used to materialize a copy that it can.

Delta itself never stores partition column values inside the Parquet data (Hive convention);
DuckLake's real `ducklake` reader recovers them by parsing `column=value` segments out of each
file's own path rather than consulting `ducklake_file_partition_value` (confirmed empirically, see
docs/IMPLEMENTATION.md). That's fine for a plain Hive-style layout, but Delta's column mapping
feature replaces those segments with opaque, unparseable directory names on Databricks -- there is
no catalog metadata fix for that without physically relocating the affected files.
"""

from __future__ import annotations

from urllib.parse import quote

HIVE_NULL_PARTITION = "__HIVE_DEFAULT_PARTITION__"


class PartitionLayoutError(RuntimeError):
    """Raised when a partitioned table's on-disk directory layout isn't `column=value` Hive style
    (the DuckDB `ducklake` reader's hard requirement for reconstructing partition values) and no
    `materialize_partitions` destination was given to work around it."""


def is_hive_style_layout(add_path: str, partition_physical_names: list[str]) -> bool:
    """Whether `add_path`'s directory segments look like genuine `physical_name=value` Hive
    partitioning, checked by key prefix only -- not exact value match, which would require
    replicating Delta's own escaping rules byte-for-byte. This only needs to catch the real failure
    mode (column mapping's opaque, `=`-less directory names), not validate encoding correctness.
    """
    if not partition_physical_names:
        return True
    segments = add_path.split("/")
    n = len(partition_physical_names)
    dir_segments = segments[-(n + 1):-1]
    if len(dir_segments) != n:
        return False
    return all(
        seg.startswith(f"{name}=")
        for seg, name in zip(dir_segments, partition_physical_names, strict=True)
    )


def encode_hive_value(value: str | None) -> str:
    """Encode one partition value the way Delta/Hive lay it out on disk: percent-encode everything
    except a small safe set that includes a literal space (confirmed against a real
    Databricks-written fixture -- `as_timestamp=2021-09-08 11%3A11%3A11`: the space between date and
    time is left bare on disk, only the colon is escaped), or the `__HIVE_DEFAULT_PARTITION__`
    sentinel for a NULL partition value.
    """
    if value is None:
        return HIVE_NULL_PARTITION
    return quote(value, safe=" ")


def hive_path_segments(
    partition_physical_names: list[str], partition_values: dict[str, str | None]
) -> list[str]:
    """Build one `physical_name=encoded_value` directory segment per partition column, in the same
    order as `partition_physical_names` (Delta's own nesting order, one directory level per
    column)."""
    return [
        f"{name}={encode_hive_value(partition_values.get(name))}"
        for name in partition_physical_names
    ]
