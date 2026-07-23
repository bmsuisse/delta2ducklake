"""Entra ID (Azure AD) token auth for Azure Database for PostgreSQL, mirroring the pattern used
by `bmsuisse/pgdevkit` -- an AAD access token is used directly as the Postgres password. Requires
the `delta2ducklake[azure]` extra (`azure-identity`).
"""

from __future__ import annotations

_default_credential = None
_managed_identity_credential = None


def get_azure_postgres_password(
    *,
    managed_identity: bool = False,
    exclude_interactive_browser_credential: bool = True,
) -> str:
    """Fetch an Entra ID token to use as an Azure Database for PostgreSQL password.

    `managed_identity=True` uses `ManagedIdentityCredential` (for workloads running under an
    Azure-assigned identity); otherwise `DefaultAzureCredential`, whose credential chain already
    falls back to managed identity when no other credential is available. Both credential objects
    are process-cached so repeated calls (one per new connection/token refresh) don't re-probe the
    credential chain every time.
    """
    try:
        from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
    except ImportError:
        raise ImportError("Install the azure extra: pip install delta2ducklake[azure]") from None

    global _default_credential, _managed_identity_credential
    if managed_identity:
        if _managed_identity_credential is None:
            _managed_identity_credential = ManagedIdentityCredential()
        credential = _managed_identity_credential
    else:
        if _default_credential is None:
            _default_credential = DefaultAzureCredential(
                exclude_interactive_browser_credential=exclude_interactive_browser_credential
            )
        credential = _default_credential
    token = credential.get_token("https://ossrdbms-aad.database.windows.net/.default")
    return token.token
