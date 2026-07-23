"""Minimal storage abstraction: local filesystem and Azure Blob/ADLS Gen2.

Delta and DuckLake both reference files by a path relative to a table root (or, more rarely, an
absolute path/URI). This module only needs to answer "give me the bytes of this file" plus a
handful of listing/range operations used for checkpoint discovery and deletion-vector sidecars —
nothing fancier, and no `fsspec` dependency.
"""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable
from urllib.parse import unquote, urlsplit


class StorageError(Exception):
    """Raised when a read against a storage backend fails (missing file, network error, ...)."""


@runtime_checkable
class StorageBackend(Protocol):
    def read_bytes(self, path: str) -> bytes: ...

    def read_range(self, path: str, offset: int, length: int) -> bytes: ...

    def list_dir(self, path: str) -> list[str]:
        """Non-recursive listing of file names (not full paths) directly under `path`."""
        ...

    def exists(self, path: str) -> bool: ...

    def resolve(self, table_root: str, relative_path: str) -> str:
        """Join `table_root` with a Delta-style (percent-encoded, forward-slash) relative path."""
        ...

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write `data` to `path`, creating any missing parent directories/prefixes.

        The only writer of *new* files in this project (deletion-vector positional-delete
        Parquet files, written into the DuckLake catalog's own managed `data_path` -- never into
        the source Delta table's directory, which this project otherwise only ever reads from).
        """
        ...


class LocalStorageBackend:
    """Reads from the local filesystem. `path` is a plain filesystem path."""

    def read_bytes(self, path: str) -> bytes:
        try:
            return Path(path).read_bytes()
        except OSError as e:
            raise StorageError(str(e)) from e

    def read_range(self, path: str, offset: int, length: int) -> bytes:
        try:
            with open(path, "rb") as f:
                f.seek(offset)
                return f.read(length)
        except OSError as e:
            raise StorageError(str(e)) from e

    def list_dir(self, path: str) -> list[str]:
        p = Path(path)
        if not p.is_dir():
            return []
        return sorted(entry.name for entry in p.iterdir() if entry.is_file())

    def exists(self, path: str) -> bool:
        return Path(path).exists()

    def resolve(self, table_root: str, relative_path: str) -> str:
        decoded = unquote(relative_path)
        return str(Path(table_root, *decoded.split("/")))

    def write_bytes(self, path: str, data: bytes) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)


class AzureStorageBackend:
    """Reads from Azure Blob Storage / ADLS Gen2 via `azure-storage-blob`.

    Accepts table roots / paths in either ``https://<account>.blob.core.windows.net/<container>/
    <blob path>`` form or ``abfss://<container>@<account>.dfs.core.windows.net/<blob path>`` form
    (ADLS Gen2); both address the same underlying blob storage so are normalized to the same client.
    """

    def __init__(self, credential=None):
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as e:
            raise ImportError(
                "Azure storage support requires the 'azure' extra: "
                "pip install delta2ducklake[azure]"
            ) from e
        self._BlobServiceClient = BlobServiceClient
        self._credential = credential
        self._container_clients: dict[str, object] = {}

    @staticmethod
    def _parse(path: str) -> tuple[str, str, str]:
        """Return (account_url, container, blob_name) from an https:// or abfss:// URL."""
        parts = urlsplit(path)
        if parts.scheme in ("abfss", "abfs"):
            container, account_host = parts.netloc.split("@", 1)
            account_host = account_host.replace(".dfs.", ".blob.")
            account_url = f"https://{account_host}"
            blob_name = parts.path.lstrip("/")
        elif parts.scheme == "https":
            account_url = f"https://{parts.netloc}"
            segments = parts.path.lstrip("/").split("/", 1)
            container = segments[0]
            blob_name = segments[1] if len(segments) > 1 else ""
        else:
            raise ValueError(f"Unsupported Azure path scheme: {path!r}")
        return account_url, container, blob_name

    def _container_client(self, account_url: str, container: str):
        key = f"{account_url}/{container}"
        if key not in self._container_clients:
            service = self._BlobServiceClient(account_url, credential=self._credential)
            self._container_clients[key] = service.get_container_client(container)
        return self._container_clients[key]

    def read_bytes(self, path: str) -> bytes:
        account_url, container, blob_name = self._parse(path)
        client = self._container_client(account_url, container)
        try:
            return client.download_blob(blob_name).readall()
        except Exception as e:
            raise StorageError(str(e)) from e

    def read_range(self, path: str, offset: int, length: int) -> bytes:
        account_url, container, blob_name = self._parse(path)
        client = self._container_client(account_url, container)
        try:
            return client.download_blob(blob_name, offset=offset, length=length).readall()
        except Exception as e:
            raise StorageError(str(e)) from e

    def list_dir(self, path: str) -> list[str]:
        account_url, container, prefix = self._parse(path)
        if prefix and not prefix.endswith("/"):
            prefix += "/"
        client = self._container_client(account_url, container)
        names = []
        for blob in client.list_blobs(name_starts_with=prefix):
            rel = blob.name[len(prefix):]
            if rel and "/" not in rel:
                names.append(rel)
        return sorted(names)

    def exists(self, path: str) -> bool:
        account_url, container, blob_name = self._parse(path)
        client = self._container_client(account_url, container)
        return bool(client.get_blob_client(blob_name).exists())

    def resolve(self, table_root: str, relative_path: str) -> str:
        decoded = unquote(relative_path)
        return f"{table_root.rstrip('/')}/{decoded}"

    def write_bytes(self, path: str, data: bytes) -> None:
        account_url, container, blob_name = self._parse(path)
        client = self._container_client(account_url, container)
        try:
            client.upload_blob(blob_name, data, overwrite=True)
        except Exception as e:
            raise StorageError(str(e)) from e


def get_storage_backend(table_root: str, *, credential=None) -> StorageBackend:
    """Pick a `StorageBackend` for `table_root` based on its URI scheme (or lack thereof).

    `credential` is forwarded to `AzureStorageBackend` (e.g. an `azure.core.credentials.
    TokenCredential`) for accounts that don't allow anonymous access -- ignored for local paths.
    """
    scheme = urlsplit(table_root).scheme
    if scheme in ("", "file"):
        return LocalStorageBackend()
    if scheme in ("https", "abfss", "abfs"):
        return AzureStorageBackend(credential=credential)
    raise ValueError(f"Unsupported storage scheme {scheme!r} for path {table_root!r}")
