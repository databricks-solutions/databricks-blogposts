"""POST ``/serving-endpoints/{name}/invocations`` and return **raw** JSON.

The Databricks Python SDK ``serving_endpoints.query`` builds a :class:`~databricks.sdk.service.serving.QueryEndpointResponse`
that only maps a fixed set of keys (``outputs``, ``predictions``, …). Agent / Responses-style payloads often
use ``output`` (singular); that field is **dropped** by ``QueryEndpointResponse.from_dict``, so ``as_dict()``
can look like ``{"id": "resp_..."}`` while traces still show the full reply. This module calls the same HTTP
path the SDK uses and returns the full response body as a ``dict``.
"""

from __future__ import annotations

from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.client_types import HostType


def post_serving_endpoint_invocations(
    user_client: WorkspaceClient,
    endpoint_name: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    """
    Same wire format as ``WorkspaceClient.serving_endpoints.query`` (body keys ``input`` / ``inputs``),
    but returns the unmarshalled JSON object without losing unknown keys.
    """
    api = user_client.serving_endpoints._api
    headers: dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    cfg = api._cfg
    if getattr(cfg, "host_type", None) == HostType.UNIFIED and getattr(cfg, "workspace_id", None):
        headers["X-Databricks-Org-Id"] = cfg.workspace_id
    result = api.do(
        "POST",
        f"/serving-endpoints/{endpoint_name}/invocations",
        body=body,
        headers=headers,
    )
    if not isinstance(result, dict):
        return {"_raw": result}
    return result
