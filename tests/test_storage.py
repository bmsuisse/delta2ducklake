from delta2ducklake.storage import (
    AzureStorageBackend,
    LocalStorageBackend,
    StorageError,
    get_storage_backend,
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
    try:
        backend.read_bytes(str(tmp_path / "nope.txt"))
        assert False, "expected StorageError"
    except StorageError:
        pass


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
