from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, UploadFile, File

from server.models.schemas import Document
from server.services.databricks_client import (
    delete_from_volume,
    execute_query,
    execute_write,
    table,
    upload_to_volume,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/documents", tags=["documents"])

# Uploads are read into memory before going to the volume, so cap their size
MAX_UPLOAD_BYTES = 50 * 1024 * 1024

# Track background processing tasks: {document_id: {status, result?, error?}}
_processing_status: dict[str, dict] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.post("/{matter_id}/upload", response_model=Document)
async def upload_document(matter_id: str, file: UploadFile = File(...)):
    # Verify matter exists
    rows = execute_query(f"SELECT matter_id FROM {table('matters')} WHERE matter_id = :matter_id", {"matter_id": matter_id})
    if not rows:
        raise HTTPException(404, "Matter not found")

    doc_id = str(uuid.uuid4())
    now = _now()
    content = await file.read(MAX_UPLOAD_BYTES + 1)
    if len(content) > MAX_UPLOAD_BYTES:
        raise HTTPException(413, f"File is larger than {MAX_UPLOAD_BYTES // (1024 * 1024)} MB")
    file_size = len(content)

    # Determine document type from extension
    fname = file.filename or "unknown"
    ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
    doc_type = "other"
    if ext == "pdf":
        doc_type = "legal_filing"
    elif ext in ("doc", "docx"):
        doc_type = "contract"
    elif ext == "txt":
        doc_type = "correspondence"

    # Upload raw file to UC Volume for ai_parse_document
    vol_path = ""
    try:
        vol_path = upload_to_volume(matter_id, f"{doc_id}_{fname}", content)
    except Exception:
        # Best-effort: processing falls back to the text stored below
        logger.exception("Volume upload failed for %s", fname)

    execute_write(
        f"""INSERT INTO {table('documents')}
        (document_id, matter_id, file_name, document_type, content_type, file_size, volume_path, uploaded_at)
        VALUES (:doc_id, :matter_id, :fname, :doc_type,
        :stream, :file_size,
        :vol_path, :now)""", {"doc_id": doc_id, "matter_id": matter_id, "fname": fname, "doc_type": doc_type, "stream": (file.content_type or "application/octet-stream"), "vol_path": vol_path, "now": now, "file_size": int(file_size)}
    )

    # Store raw content as text (for text files) or placeholder for binary
    text_content = ""
    try:
        text_content = content.decode("utf-8")
    except UnicodeDecodeError:
        text_content = f"[Binary file stored at: {vol_path}]"

    execute_write(
        f"""INSERT INTO {table('document_contents')} (document_id, content)
        VALUES (:doc_id, :text_content)""", {"doc_id": doc_id, "text_content": text_content}
    )

    rows = execute_query(f"SELECT * FROM {table('documents')} WHERE document_id = :doc_id", {"doc_id": doc_id})
    return _row_to_doc(rows[0])


@router.get("/{matter_id}", response_model=list[Document])
async def list_documents(matter_id: str):
    rows = execute_query(
        f"SELECT * FROM {table('documents')} WHERE matter_id = :matter_id ORDER BY uploaded_at DESC", {"matter_id": matter_id}
    )
    return [_row_to_doc(r) for r in rows]


@router.post("/{matter_id}/{document_id}/process", status_code=202)
async def process_document(matter_id: str, document_id: str):
    """Start document processing in the background. Poll /status for progress."""
    from server.services.document_processor import process_document as _process

    rows = execute_query(f"SELECT * FROM {table('documents')} WHERE document_id = :document_id", {"document_id": document_id})
    if not rows:
        raise HTTPException(404, "Document not found")

    # If already processing, return current status
    if document_id in _processing_status and _processing_status[document_id]["status"] == "processing":
        return _processing_status[document_id]

    _processing_status[document_id] = {"status": "processing", "step": "starting"}

    async def _run():
        try:
            result = await _process(matter_id, document_id, progress_cb=lambda step: _processing_status.update({document_id: {**_processing_status.get(document_id, {}), "step": step}}))
            _processing_status[document_id] = {"status": "completed", "result": result}
        except Exception:
            logger.exception("Processing failed for document %s", document_id)
            _processing_status[document_id] = {"status": "error", "error": "Processing failed. See the app logs for details."}

    asyncio.create_task(_run())
    return _processing_status[document_id]


@router.get("/{matter_id}/{document_id}/process/status")
async def get_processing_status(matter_id: str, document_id: str):
    """Poll processing status."""
    status = _processing_status.get(document_id)
    if not status:
        # Check if already processed
        rows = execute_query(f"SELECT processed_at FROM {table('documents')} WHERE document_id = :document_id", {"document_id": document_id})
        if rows and rows[0].get("processed_at"):
            return {"status": "completed", "result": {"status": "already_processed"}}
        return {"status": "idle"}
    return status


@router.delete("/{matter_id}/{document_id}")
async def delete_document(matter_id: str, document_id: str):
    # Clean up volume file
    rows = execute_query(
        f"SELECT volume_path FROM {table('documents')} WHERE document_id = :document_id", {"document_id": document_id}
    )
    if rows and rows[0].get("volume_path"):
        delete_from_volume(rows[0]["volume_path"])

    execute_write(f"DELETE FROM {table('document_chunks')} WHERE document_id = :document_id", {"document_id": document_id})
    execute_write(f"DELETE FROM {table('document_contents')} WHERE document_id = :document_id", {"document_id": document_id})
    execute_write(f"DELETE FROM {table('documents')} WHERE document_id = :document_id", {"document_id": document_id})
    return {"deleted": True}


def _row_to_doc(r: dict) -> Document:
    return Document(
        document_id=r["document_id"],
        matter_id=r["matter_id"],
        file_name=r["file_name"],
        document_type=r.get("document_type"),
        content_type=r.get("content_type"),
        file_size=r.get("file_size"),
        page_count=r.get("page_count"),
        volume_path=r.get("volume_path"),
        uploaded_at=r["uploaded_at"],
        processed_at=r.get("processed_at"),
    )
