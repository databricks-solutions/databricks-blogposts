"""
Simple, no-deps Python client to exercise Genie Spaces import/export APIs from notebooks.

- Automatically stringifies (for requests) and destringifies (for responses) the `serialized_space` field.
- Surfaces clear errors with HTTP status codes and messages.
- If no explicit token or env vars (DB_PAT / DB_TOKEN / DATABRICKS_TOKEN) are set, will attempt to
  obtain an ephemeral, short-lived token + workspace host from the Databricks notebook context
  (dbutils / Spark) and use that. This lets the client "just work" inside a Databricks notebook
  without manual auth. The ephemeral token only works inside that session and is not persisted.

Usage:

    from genie_spaces_client import GenieSpacesClient

    client = GenieSpacesClient(
        token="dapi...", # optional; if omitted, checks env vars then notebook context
        base_url="https://e2-dogfood.staging.cloud.databricks.com/api/2.0", # optional
        traffic_id="testenv://..."  # optional
    )

    # Get a space with export
    space = client.get_space("<space_id>", include_serialized_space=True)

    # Create a new space by reusing an export
    export_obj = space.get("serialized_space")
    created = client.create_space(
        warehouse_id="<warehouse_id>",
        serialized_space=export_obj,
        title=f"import of {space.get('title', 'Unnamed')}"
    )

    # Update an existing space
    updated = client.update_space(
        space_id=created["space_id"],
        serialized_space=export_obj,
        title="Updated Title"
    )

Notes:
- This client intentionally avoids external dependencies like `requests`.
- HTTP error responses raise GenieSpacesError(status_code, message, response_body).
- Notebook-context auth fallback only triggers when no token/env tokens are present AND we're in a notebook.
"""

from typing import Any, Dict, Optional
import json
import urllib.request
import urllib.parse
import urllib.error
import os

class GenieSpacesError(Exception):
    """Error raised for non-2xx responses with status code and message."""

    def __init__(self, status_code: int, message: str, response_body: Optional[str] = None):
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.message = message
        self.response_body = response_body


class GenieSpacesClient:
    """Minimal Genie Spaces API client using Python stdlib only.

    Auth resolution order:
    1. Explicit token argument
    2. Environment variables: DB_PAT, DB_TOKEN, DATABRICKS_TOKEN
    3. Databricks notebook context (ephemeral apiToken + apiUrl) if available

    If (3) is used and no base_url is provided, base_url becomes <apiUrl>/api/2.0
    """

    def __init__(
        self,
        token: Optional[str] = None,
        base_url: Optional[str] = None,
        traffic_id: Optional[str] = None,
        timeout: int = 60,
    ):
        # First, gather an explicit or env-provided token
        resolved_token = token or os.getenv("DB_PAT") or os.getenv("DB_TOKEN") or os.getenv("DATABRICKS_TOKEN")

        base_url_was_provided = base_url is not None
        # Provisional base URL (may be overridden if we later discover notebook context & need one)
        self._base_url = (base_url or "https://e2-dogfood.staging.cloud.databricks.com/api/2.0").rstrip("/")
        self._traffic_id = traffic_id
        self._timeout = timeout

        # If no token yet, attempt notebook-context fallback (Databricks notebook only)
        if not resolved_token:
            nb_token, nb_base = self._try_notebook_context()
            if nb_token:
                resolved_token = nb_token
                # Only override base_url if user did not supply one explicitly
                if not base_url_was_provided and nb_base:
                    # Ensure trailing /api/2.0 (apiUrl() returns host root)
                    self._base_url = f"{nb_base.rstrip('/')}/api/2.0"
        self._token = resolved_token

    # ---------------------------- Public API ----------------------------

    def get_space(self, space_id: str, include_serialized_space: bool = False) -> Dict[str, Any]:
        """
        GET /genie/spaces/{space_id}

        If include_serialized_space=True, the response contains 'serialized_space' as a dict.
        """
        path = f"/genie/spaces/{space_id}"
        params = {"include_serialized_space": "true" if include_serialized_space else "false"}
        resp = self._request("GET", path, params=params)
        return self._maybe_destringify_export(resp)

    def create_space(
        self,
        *,
        warehouse_id: str,
        serialized_space: Any,
        title: Optional[str] = None,
        parent_path: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        POST /genie/spaces

        - serialized_space: dict (preferred) or JSON string. Will be stringified automatically.
        Returns response with 'serialized_space' as dict if present.
        """
        body: Dict[str, Any] = {
            "warehouse_id": warehouse_id,
            "serialized_space": self._to_export_json_string(serialized_space),
        }
        if title is not None:
            body["title"] = title
        if parent_path is not None:
            body["parent_path"] = parent_path
        if description is not None:
            body["description"] = description

        resp = self._request("POST", "/genie/spaces", body=body)
        return self._maybe_destringify_export(resp)

    def update_space(
        self,
        *,
        space_id: str,
        serialized_space: Optional[Any] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        warehouse_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        PATCH /genie/spaces/{space_id}

        Any provided field will be updated. 'serialized_space' may be dict or JSON string.
        Returns response with 'serialized_space' as dict if present.
        """
        body: Dict[str, Any] = {}
        if serialized_space is not None:
            body["serialized_space"] = self._to_export_json_string(serialized_space)
        if title is not None:
            body["title"] = title
        if description is not None:
            body["description"] = description
        if warehouse_id is not None:
            body["warehouse_id"] = warehouse_id

        resp = self._request("PATCH", f"/genie/spaces/{space_id}", body=body)
        return self._maybe_destringify_export(resp)

    # ---------------------------- Internal helpers ----------------------------

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        url = self._base_url + "/" + path.lstrip("/")
        if params:
            query = urllib.parse.urlencode(params)
            url = f"{url}?{query}"

        data_bytes: Optional[bytes] = None
        if body is not None:
            data_bytes = json.dumps(body).encode("utf-8")

        req = urllib.request.Request(url=url, data=data_bytes, method=method)
        if not self._token:
            raise GenieSpacesError(status_code=0, message="No authentication token available", response_body=None)
        req.add_header("Authorization", f"Bearer {self._token}")
        req.add_header("Content-Type", "application/json")
        if self._traffic_id:
            req.add_header("x-databricks-traffic-id", self._traffic_id)

        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                resp_body = resp.read().decode("utf-8")
                if not resp_body:
                    return {}
                return json.loads(resp_body)
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8") if e.fp else ""
            # Databricks-style error payload typically has 'error_code' and 'message'
            message = self._extract_error_message(err_body) or e.reason or "Request failed"
            # Suppress chained traceback from urllib by raising from None
            raise GenieSpacesError(status_code=e.code, message=message, response_body=err_body) from None
        except urllib.error.URLError as e:
            # Suppress chained traceback from urllib by raising from None
            raise GenieSpacesError(status_code=0, message=str(e), response_body=None) from None

    @staticmethod
    def _to_export_json_string(export_obj: Any) -> str:
        if export_obj is None:
            return "null"
        if isinstance(export_obj, (dict, list)):
            return json.dumps(export_obj)
        if isinstance(export_obj, str):
            # Validate it's JSON; if not, still pass through but avoid raising here
            try:
                json.loads(export_obj)
            except Exception:
                pass
            return export_obj
        # Fallback: try to serialize
        try:
            return json.dumps(export_obj)
        except Exception:
            # As a last resort, string-cast
            return str(export_obj)

    @staticmethod
    def _maybe_destringify_export(resp: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(resp, dict):
            return resp
        ser = resp.get("serialized_space")
        if isinstance(ser, str):
            try:
                resp["serialized_space"] = json.loads(ser)
            except Exception:
                # Leave as string if not valid JSON
                pass
        return resp

    @staticmethod
    def _extract_error_message(err_body: Optional[str]) -> Optional[str]:
        if not err_body:
            return None
        try:
            payload = json.loads(err_body)
            # Common Databricks error fields
            if isinstance(payload, dict):
                if "message" in payload and isinstance(payload["message"], str):
                    return payload["message"]
                # Some services use 'error' or nested fields
                if "error" in payload and isinstance(payload["error"], str):
                    return payload["error"]
        except Exception:
            return err_body.strip() or None
        return None

    @staticmethod
    def _try_notebook_context() -> tuple[Optional[str], Optional[str]]:
        """Attempt to pull (token, host) from Databricks notebook context.
        Returns (token, host) or (None, None) if unavailable.
        Does not raise.
        """
        # Try existing `dbutils` global first.
        dbutils_ref = globals().get("dbutils")
        ctx = None
        try:
            if dbutils_ref is None:
                # Attempt to construct DBUtils via SparkSession (safe in Databricks)
                try:
                    from pyspark.sql import SparkSession  # type: ignore
                    from pyspark.dbutils import DBUtils  # type: ignore
                    spark = SparkSession.builder.getOrCreate()
                    dbutils_ref = DBUtils(spark)
                except Exception:
                    dbutils_ref = None
            if dbutils_ref:
                # Modern path
                try:
                    ctx = dbutils_ref.notebook.entry_point.getDbutils().notebook().getContext()
                except Exception:
                    # Older style fallback
                    try:
                        ctx = dbutils_ref.notebook.getContext()
                    except Exception:
                        ctx = None
            if ctx:
                host = None
                token = None
                try:
                    host = ctx.apiUrl().get()
                except Exception:
                    host = None
                try:
                    token = ctx.apiToken().get()
                except Exception:
                    token = None
                if token and host:
                    return token, host
                if token:
                    return token, None
        except Exception:
            pass
        return None, None

__all__ = ["GenieSpacesClient", "GenieSpacesError"]