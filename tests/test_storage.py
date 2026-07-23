import pytest

from delta2ducklake.storage import (
    AzureStorageBackend,
    LocalStorageBackend,
    StorageError,
    get_storage_backend,
    to_duckdb_uri,
)


def test_local_backend_read_bytes(tmp_path):
    f = tmp_path / "hello.txt"
    f.write_bytes(b"hello world")
    backend = LocalStorageBackend()
    assert backend.read_bytes(str(f)) == b"hello world"


def test_local_backend_read_range(tmp_path):
    f = tmp_path / "hello.txt"
    f.write_bytes(b"0123456789")
    backend = LocalStorageBackend()
    assert backend.read_range(str(f), 3, 4) == b"3456"


def test_local_backend_missing_file_raises_storage_error(tmp_path):
    backend = LocalStorageBackend()
    with pytest.raises(StorageError):
        backend.read_bytes(str(tmp_path / "nope.txt"))


def test_local_backend_list_dir(tmp_path):
    (tmp_path / "a.parquet").write_bytes(b"")
    (tmp_path / "b.parquet").write_bytes(b"")
    (tmp_path / "sub").mkdir()
    backend = LocalStorageBackend()
    assert backend.list_dir(str(tmp_path)) == ["a.parquet", "b.parquet"]


def test_local_backend_resolve_decodes_percent_encoding(tmp_path):
    backend = LocalStorageBackend()
    resolved = backend.resolve(str(tmp_path), "part%3Da/b%20c.parquet")
    assert resolved == str(tmp_path / "part=a" / "b c.parquet")


def test_get_storage_backend_local_for_plain_path():
    assert isinstance(get_storage_backend("/some/path"), LocalStorageBackend)
    assert isinstance(get_storage_backend("file:///some/path"), LocalStorageBackend)


def test_get_storage_backend_azure_for_https_and_abfss():
    assert isinstance(
        get_storage_backend("https://myaccount.blob.core.windows.net/mycontainer/path"),
        AzureStorageBackend,
    )
    assert isinstance(
        get_storage_backend("abfss://mycontainer@myaccount.dfs.core.windows.net/path"),
        AzureStorageBackend,
    )


def test_azure_backend_parse_https_and_abfss_agree():
    https_parsed = AzureStorageBackend._parse(
        "https://myaccount.blob.core.windows.net/mycontainer/some/blob.parquet"
    )
    abfss_parsed = AzureStorageBackend._parse(
        "abfss://mycontainer@myaccount.dfs.core.windows.net/some/blob.parquet"
    )
    assert https_parsed == abfss_parsed == (
        "https://myaccount.blob.core.windows.net",
        "mycontainer",
        "some/blob.parquet",
    )


def test_to_duckdb_uri_rewrites_databricks_style_abfss():
    assert (
        to_duckdb_uri("abfss://mycontainer@myaccount.dfs.core.windows.net/some/path")
        == "abfss://myaccount.dfs.core.windows.net/mycontainer/some/path"
    )


def test_to_duckdb_uri_rewrites_bare_container_root():
    assert (
        to_duckdb_uri("abfss://mycontainer@myaccount.dfs.core.windows.net/")
        == "abfss://myaccount.dfs.core.windows.net/mycontainer/"
    )


def test_to_duckdb_uri_handles_abfs_scheme_too():
    assert (
        to_duckdb_uri("abfs://mycontainer@myaccount.dfs.core.windows.net/some/path")
        == "abfs://myaccount.dfs.core.windows.net/mycontainer/some/path"
    )


def test_to_duckdb_uri_leaves_already_duckdb_style_abfss_unchanged():
    already_ok = "abfss://myaccount.dfs.core.windows.net/mycontainer/some/path"
    assert to_duckdb_uri(already_ok) == already_ok


def test_to_duckdb_uri_leaves_other_schemes_unchanged():
    https_path = "https://myaccount.blob.core.windows.net/mycontainer/some/path"
    local_path = "/some/local/path"
    assert to_duckdb_uri(https_path) == https_path
    assert to_duckdb_uri(local_path) == local_path
