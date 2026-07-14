import base64
import io
import traceback
from datetime import datetime
from typing import Annotated, Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.iam import User as UserOut
from fastapi import APIRouter, Depends, HTTPException, Request

from .._metadata import api_prefix
from .agent_server.agent import chat_supervisor_query
from .config import AppConfig, get_access_token_diagnostic
from .serving_endpoint_metadata import (
    log_obo_scope_hint,
    obo_details_for_403_detail,
    get_serving_endpoint_obo_details,
)
from .workspace_auth import get_supervisor_endpoint_name
from .dependencies import ConfigDep, get_job_workspace_client, get_obo_ws, get_volume_obo_ws, get_volume_token
from .logger import logger
from .models import (
    AppAiQueryOut,
    ChatIn,
    ChatOut,
    FileInfo,
    FileListOut,
    FilesUploadIn,
    JobRunOut,
    JobRunTriggerOut,
    VersionOut,
)

api = APIRouter(prefix=api_prefix)


def _as_int_ms_epoch(v) -> int | None:
    """Coerce SDK run timestamps to int ms for JSON / Pydantic (avoids response validation 500s)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _databricks_error_code_upper(e: DatabricksError) -> str:
    """Normalize ``DatabricksError.error_code`` (str or int from the platform) for comparisons."""
    raw = getattr(e, "error_code", None)
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw.upper()
    return str(raw).upper()


def _http_status_for_databricks_error(e: DatabricksError) -> int | None:
    """Map common Databricks API failures to HTTP status codes."""
    code = _databricks_error_code_upper(e)
    msg = (str(e) or "").lower()
    if (
        "PERMISSION" in code
        or "PERMISSION_DENIED" in code
        or code == "403"
        or "403" in msg
        or "forbidden" in msg
        or "required scopes" in msg
    ):
        return 403
    if (
        "NOT_FOUND" in code
        or "DOES_NOT_EXIST" in code
        or "RESOURCE_DOES_NOT_EXIST" in code
        or ("INVALID_PARAMETER" in code and "job" in msg)
    ):
        return 404
    return None


def _http_exception_from_agent_failure(
    exc: Exception,
    *,
    config: AppConfig | None = None,
    request: Request | None = None,
    agent_endpoint_name: str | None = None,
) -> HTTPException:
    """Structured error for /api/chat when ``serving_endpoints.query`` fails."""
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, DatabricksError):
        status = _http_status_for_databricks_error(exc) or 502
        detail: dict[str, Any] = {
            "message": str(exc),
            "error_code": exc.error_code,
            "error_type": type(exc).__name__,
            "hint": (
                "Check that AGENT_ENDPOINT names a serving endpoint in this workspace, "
                "the user token has required OAuth scopes (e.g. model-serving), and the user has CAN_QUERY on the endpoint."
            ),
        }
        if (
            status == 403
            and config is not None
            and (agent_endpoint_name or "").strip()
        ):
            name = (agent_endpoint_name or "").strip()
            obo = get_serving_endpoint_obo_details(name, config=config, request=request)
            log_obo_scope_hint(name, obo)
            detail.update(obo_details_for_403_detail(obo))
        return HTTPException(
            status_code=status,
            detail=detail,
        )
    return HTTPException(
        status_code=502,
        detail={
            "message": str(exc),
            "error_type": type(exc).__name__,
            "hint": "Unexpected error while calling serving_endpoints.query.",
        },
    )


class _BytesIOWithLen(io.BytesIO):
    """BytesIO that supports len() for SDKs that require it (e.g. Databricks files.upload)."""

    def __len__(self) -> int:
        return len(self.getvalue())


@api.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    return VersionOut.from_metadata()


@api.get("/auth/diagnostic", operation_id="authDiagnostic")
def auth_diagnostic(request: Request, config: ConfigDep):
    """
    Check whether x-forwarded-access-token is present and if a token can be resolved.
    Use this when auth fails on Databricks to see if the header is being sent.
    Does not return any token value.
    """
    return get_access_token_diagnostic(request, config)


@api.get("/current-user", response_model=UserOut, operation_id="currentUser")
def me(obo_ws: Annotated[WorkspaceClient, Depends(get_obo_ws)]):
    return obo_ws.current_user.me()


@api.get("/files", response_model=FileListOut, operation_id="listFiles")
def list_files(
    config: ConfigDep,
    ws: Annotated[WorkspaceClient, Depends(get_volume_obo_ws)],
):
    """List PDF files in the configured volume (using OBO token from X-Forwarded-Access-Token)."""
    volume_path = config.volume_path
    logger.info(f"[LIST] Listing files from volume: {volume_path}")
    logger.info(f"[LIST] Using host: {config.host}")

    files: list[FileInfo] = []
    try:
        logger.info(f"[LIST] Calling ws.files.list_directory_contents({volume_path})")
        for file_info in ws.files.list_directory_contents(volume_path):
            logger.info(f"[LIST] Found file: {file_info.name}")
            if file_info.name and file_info.name.lower().endswith(".pdf"):
                files.append(
                    FileInfo(
                        name=file_info.name,
                        path=file_info.path or "",
                        size=file_info.file_size or 0,
                        modified_at=datetime.fromtimestamp(file_info.last_modified / 1000)
                        if file_info.last_modified
                        else None,
                    )
                )
        logger.info(f"[LIST] Found {len(files)} PDF files")
    except Exception as e:
        logger.error(f"[LIST] Error listing files: {type(e).__name__}: {e}")
        err_msg = str(e)
        if "required scopes" in err_msg.lower() and "files" in err_msg.lower():
            raise HTTPException(
                status_code=403,
                detail={
                    "message": "The token does not have the 'files' scope.",
                    "hint": "The token must have the Databricks 'files' scope to list/upload files.",
                    "fix": "Ensure the signed-in user's token (Apps) or PAT (local) includes the Databricks 'files' scope. For PATs: User Settings → Developer → Access tokens with Files enabled.",
                },
            ) from e
        raise HTTPException(status_code=502, detail=f"Failed to list files: {e!s}") from e

    return FileListOut(files=files)


@api.post("/files", response_model=FileListOut, operation_id="uploadFiles")
def upload_files(
    config: ConfigDep,
    ws: Annotated[WorkspaceClient, Depends(get_volume_obo_ws)],
    payload: FilesUploadIn,
):
    """Upload PDF files to the configured volume (using OBO token from X-Forwarded-Access-Token)."""
    volume_path = config.volume_path
    logger.info(f"[UPLOAD] Received upload request with {len(payload.files)} files")
    logger.info(f"[UPLOAD] Volume path: {volume_path}")
    logger.info(f"[UPLOAD] Host: {config.host}")

    uploaded: list[FileInfo] = []

    for file in payload.files:
        logger.info(f"[UPLOAD] Processing file: {file.name}")
        logger.info(f"[UPLOAD] Base64 content length: {len(file.content_base64)}")
        
        # Only accept PDF files
        if not file.name.lower().endswith(".pdf"):
            logger.warning(f"[UPLOAD] Skipping non-PDF file: {file.name}")
            continue

        try:
            # Decode base64 content
            logger.info("[UPLOAD] Decoding base64 content...")
            content = base64.b64decode(file.content_base64)
            logger.info(f"[UPLOAD] Decoded content size: {len(content)} bytes")
            
            file_path = f"{volume_path}/{file.name}"
            logger.info(f"[UPLOAD] Uploading to: {file_path}")

            # SDK expects a file-like object and may call len() on it; BytesIO has no __len__
            content_stream = _BytesIOWithLen(content)
            ws.files.upload(file_path, content_stream, overwrite=True)
            logger.info(f"[UPLOAD] Successfully uploaded: {file.name}")

            uploaded.append(
                FileInfo(
                    name=file.name,
                    path=file_path,
                    size=len(content),
                    modified_at=datetime.now(),
                )
            )
        except Exception as e:
            logger.error(f"[UPLOAD] Error uploading {file.name}: {type(e).__name__}: {e}")
            logger.error(traceback.format_exc())
            err_msg = str(e)
            if "required scopes" in err_msg.lower() and "files" in err_msg.lower():
                raise HTTPException(
                    status_code=403,
                    detail={
                        "message": f"Upload failed for {file.name}: the token does not have the 'files' scope.",
                        "hint": "The token (from x-forwarded-access-token or environment) must have the Databricks 'files' scope to use the Files API.",
                        "fix": "Ensure the signed-in user's token (Apps) or PAT (local) includes the 'files' scope. For PATs: User Settings → Developer → Access tokens with Files enabled.",
                    },
                ) from e
            raise HTTPException(
                status_code=502,
                detail=f"Upload failed for {file.name}: {e!s}",
            ) from e

    logger.info(f"[UPLOAD] Upload complete. Uploaded {len(uploaded)} files")
    return FileListOut(files=uploaded)


@api.post("/jobs/run", response_model=JobRunTriggerOut, operation_id="triggerJobRun")
def trigger_job_run(
    config: ConfigDep,
    ws: Annotated[WorkspaceClient, Depends(get_job_workspace_client)],
):
    """Trigger the configured processing job (run now). Returns run_id for polling.

    Uses the app service principal on Databricks Apps (see ``get_job_workspace_client``): ``jobs`` is not
    a valid ``user_api_scopes`` entry in the bundle, so OBO tokens typically lack the Jobs OAuth scope.
    """
    raw_job = (config.processing_job_id or "").strip()
    if not raw_job:
        raise HTTPException(
            status_code=400,
            detail=(
                "processing_job_id is not set. Configure JOB_ID "
                "(bundle app config.env / workspace app settings) to the numeric Databricks job ID."
            ),
        )
    try:
        job_id = int(raw_job)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid processing_job_id in config (expected integer job id): {raw_job!r}",
        )
    try:
        waiter = ws.jobs.run_now(job_id=job_id)
        resp = waiter.response
        raw_run_id = getattr(resp, "run_id", None) if resp is not None else None
        if raw_run_id is None:
            raise HTTPException(
                status_code=502,
                detail=(
                    "Job run was submitted but the workspace returned no run_id. "
                    "Check processing_job_id, Jobs API access, and that config.host matches the workspace."
                ),
            )
        run_id = int(raw_run_id)
        logger.info(f"[JOB] Triggered job_id={job_id}, run_id={run_id}")
        return JobRunTriggerOut(run_id=run_id, job_id=config.processing_job_id)
    except HTTPException:
        raise
    except DatabricksError as e:
        logger.error(f"[JOB] Error triggering job: {type(e).__name__}: {e}")
        logger.error(traceback.format_exc())
        mapped = _http_status_for_databricks_error(e)
        if mapped is not None:
            err_lower = str(e).lower()
            if "required scopes" in err_lower and "jobs" in err_lower:
                job_hint = (
                    "The token used for Jobs API calls is missing the 'jobs' OAuth scope. "
                    "On Databricks Apps, /api/jobs/* should use the app service principal "
                    "(DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET); the bundle cannot declare `jobs` "
                    "under user_api_scopes (invalid scope id). "
                    "Locally, set those env vars or use a PAT with Jobs scope for FEVM_TOKEN / DATA_EXTRACTION_TOKEN."
                )
            else:
                job_hint = (
                    "Grant the signed-in user (OBO token) CAN_MANAGE_RUN or CAN_MANAGE on this job, "
                    "or ensure their token can run the job. "
                    "Ensure processing_job_id exists in the same workspace as config.host."
                )
            raise HTTPException(
                status_code=mapped,
                detail={
                    "message": str(e),
                    "error_code": e.error_code,
                    "hint": job_hint,
                },
            ) from e
        raise HTTPException(status_code=502, detail=f"Failed to trigger job: {e!s}") from e
    except Exception as e:
        logger.error(f"[JOB] Error triggering job: {type(e).__name__}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=502, detail=f"Failed to trigger job: {e!s}") from e


@api.get("/jobs/runs/{run_id}", response_model=JobRunOut, operation_id="getJobRun")
def get_job_run(
    run_id: int,
    ws: Annotated[WorkspaceClient, Depends(get_job_workspace_client)],
):
    """Get current status and timing of a job run (same auth as ``POST /jobs/run``)."""
    try:
        run = ws.jobs.get_run(run_id=run_id)
    except DatabricksError as e:
        logger.error(f"[JOB] Error getting run {run_id}: {type(e).__name__}: {e}")
        mapped = _http_status_for_databricks_error(e)
        status = mapped if mapped is not None else 502
        raise HTTPException(status_code=status, detail=str(e)) from e
    except Exception as e:
        logger.error(f"[JOB] Error getting run {run_id}: {type(e).__name__}: {e}")
        raise HTTPException(status_code=404, detail=f"Run not found or error: {e!s}") from e
    state = getattr(run, "state", None) or getattr(run, "status", None)
    life_cycle_state = (
        getattr(state, "life_cycle_state", None)
        or getattr(run, "life_cycle_state", None)
        or "UNKNOWN"
    )
    result_state = getattr(state, "result_state", None) or getattr(run, "result_state", None)
    start_time = getattr(run, "start_time", None)
    end_time = getattr(run, "end_time", None)

    def _enum_value_name(v):
        if v is None:
            return None
        if hasattr(v, "name"):
            return getattr(v, "name")
        s = str(v)
        return s.split(".")[-1] if "." in s else s

    life_cycle_state_str = _enum_value_name(life_cycle_state) or "UNKNOWN"
    result_state_str = _enum_value_name(result_state)

    raw_rid = getattr(run, "run_id", None)
    effective_run_id = int(raw_rid) if raw_rid is not None else run_id

    st = _as_int_ms_epoch(start_time)
    et = _as_int_ms_epoch(end_time)
    execution_duration_ms = None
    if st is not None and et is not None and et > 0:
        execution_duration_ms = et - st
    return JobRunOut(
        run_id=effective_run_id,
        life_cycle_state=life_cycle_state_str,
        result_state=result_state_str,
        start_time=st,
        end_time=et if (et is not None and et > 0) else None,
        execution_duration_ms=execution_duration_ms,
    )


def _row_to_json_serializable(row: tuple, columns: list[str]) -> dict:
    """Convert a row tuple to a dict with JSON-serializable values."""
    out = {}
    for i, col in enumerate(columns):
        if i >= len(row):
            break
        v = row[i]
        if v is None:
            out[col] = None
        elif isinstance(v, (datetime,)):
            out[col] = v.isoformat()
        elif hasattr(v, "__float__") and not isinstance(v, (int, float, bool)):
            out[col] = str(v)
        else:
            out[col] = v
    return out


@api.get("/query/app_ai_query", response_model=AppAiQueryOut, operation_id="getAppAiQuery")
def get_app_ai_query(
    config: ConfigDep,
    token: Annotated[str, Depends(get_volume_token)],
):
    """Query the app_ai_query table via SQL Warehouse and return rows."""
    from databricks import sql as databricks_sql

    server_hostname = (config.host or "").replace("https://", "").replace("http://", "")
    if not server_hostname or not config.warehouse_http_path or not token:
        raise HTTPException(
            status_code=500,
            detail="SQL Warehouse configuration or token missing",
        )
    def _quote_table(name: str) -> str:
        return ".".join("`" + part.replace("`", "``") + "`" for part in name.split("."))

    try:
        with databricks_sql.connect(
            server_hostname=server_hostname,
            http_path=config.warehouse_http_path,
            access_token=token,
        ) as conn:
            with conn.cursor() as cursor:
                table = config.app_ai_query_table
                quoted_table = _quote_table(table)
                cursor.execute(f"SELECT * FROM {quoted_table} LIMIT 1000")
                columns = [d[0] for d in (cursor.description or [])]
                raw_rows = cursor.fetchall()
        rows = [_row_to_json_serializable(r, columns) for r in raw_rows]
        return AppAiQueryOut(rows=rows, columns=columns)
    except Exception as e:
        logger.error(f"[QUERY] Error querying app_ai_query: {type(e).__name__}: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=502, detail=f"Query failed: {e!s}") from e


@api.post("/chat", response_model=ChatOut, operation_id="chat")
def chat(
    request: Request,
    config: ConfigDep,
    payload: ChatIn,
):
    """Send messages via ``get_user_workspace_client()`` → ``user_client.serving_endpoints.query``.

    The user token comes only from ``x-forwarded-access-token`` on Apps (not ``get_volume_token`` /
    env PAT), so serving is attributed to the signed-in user.

    MLflow AgentServer uses the same client logic: ``POST /api/agent/invocations`` or ``POST /api/agent/responses``.
    """
    if not payload.messages:
        raise HTTPException(status_code=400, detail="messages cannot be empty")
    api_messages = [{"role": m.role, "content": m.content} for m in payload.messages]

    endpoint = (config.agent_endpoint or get_supervisor_endpoint_name() or "").strip()
    try:
        msg_out = chat_supervisor_query(request, config, api_messages)
        return ChatOut(message=msg_out)
    except Exception as e:
        logger.error(
            "[CHAT] serving_endpoints.query failed: %s: %s",
            type(e).__name__,
            e,
            exc_info=True,
        )
        raise _http_exception_from_agent_failure(
            e,
            config=config,
            request=request,
            agent_endpoint_name=endpoint or None,
        ) from e
