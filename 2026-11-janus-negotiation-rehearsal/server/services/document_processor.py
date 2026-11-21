from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from server.config import settings
from server.services.databricks_client import execute_query, execute_write, table
from server.services.foundation_model import chat_completion_json, generate_embeddings

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
EMBED_BATCH_SIZE = 20


async def process_document(
    matter_id: str,
    document_id: str,
    progress_cb: Any = None,
) -> dict[str, Any]:
    """Process a document: parse via ai_parse_document, chunk, embed, extract entities + parties."""

    def _progress(step: str):
        logger.info("Document %s: %s", document_id, step)
        if progress_cb:
            progress_cb(step)

    # 1. Get document metadata (need volume_path)
    _progress("loading metadata")
    doc_rows = execute_query(
        f"SELECT * FROM {table('documents')} WHERE document_id = :document_id", {"document_id": document_id}
    )
    if not doc_rows:
        return {"status": "error", "message": "Document not found"}

    doc = doc_rows[0]
    vol_path = doc.get("volume_path")

    # 2. Extract text via ai_parse_document (if volume_path exists) or fallback
    _progress("parsing document")
    text = ""
    if vol_path:
        text = _parse_document_from_volume(vol_path)

    if not text:
        # Fallback: read from document_contents (text files, legacy uploads)
        rows = execute_query(
            f"SELECT content FROM {table('document_contents')} WHERE document_id = :document_id", {"document_id": document_id}
        )
        text = rows[0].get("content", "") if rows else ""

    if not text or text.startswith("[Binary file"):
        return {"status": "error", "message": "No extractable content found in document"}

    _progress(f"parsed {len(text)} chars")

    # Update document_contents with parsed text for downstream consumers
    execute_write(
        f"DELETE FROM {table('document_contents')} WHERE document_id = :document_id", {"document_id": document_id}
    )
    execute_write(
        f"INSERT INTO {table('document_contents')} (document_id, content) VALUES (:document_id, :safe_text)", {"document_id": document_id, "safe_text": text}
    )

    # 3. Chunk text
    chunks = _chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP)
    _progress(f"chunked into {len(chunks)} chunks")

    # 4. Generate embeddings in batches
    _progress("generating embeddings")
    all_embeddings: list[list[float]] = []
    for i in range(0, len(chunks), EMBED_BATCH_SIZE):
        batch = chunks[i : i + EMBED_BATCH_SIZE]
        _progress(f"embedding batch {i // EMBED_BATCH_SIZE + 1}/{(len(chunks) + EMBED_BATCH_SIZE - 1) // EMBED_BATCH_SIZE}")
        try:
            embs = await generate_embeddings(batch)
            all_embeddings.extend(embs)
        except Exception as e:
            logger.warning("Embedding generation failed for batch %d: %s", i, e)
            all_embeddings.extend([[] for _ in batch])

    # 5. Store chunks (batched for performance)
    _progress("storing chunks")
    execute_write(f"DELETE FROM {table('document_chunks')} WHERE document_id = :document_id", {"document_id": document_id})

    CHUNK_BATCH = 10
    for batch_start in range(0, len(chunks), CHUNK_BATCH):
        batch_end = min(batch_start + CHUNK_BATCH, len(chunks))
        stmts = []
        for idx in range(batch_start, batch_end):
            chunk_text = chunks[idx]
            embedding = all_embeddings[idx] if idx < len(all_embeddings) else []
            chunk_id = str(uuid.uuid4())
            # Embedding values are model output: force each to a float so nothing else reaches the SQL
            emb_str = f"ARRAY({','.join(repr(float(x)) for x in embedding)})" if embedding else "ARRAY()"
            stmts.append(
                (f"""INSERT INTO {table('document_chunks')}
                (chunk_id, document_id, matter_id, chunk_index, text, embedding)
                VALUES (:chunk_id, :document_id, :matter_id, :idx,
                :chunk_text, {emb_str})""", {"chunk_id": chunk_id, "document_id": document_id, "matter_id": matter_id, "chunk_text": chunk_text, "idx": int(idx)})
            )
        # Execute batch
        for sql_text, params in stmts:
            execute_write(sql_text, params)

    # 6. LLM extraction (issues, evidence, timeline, AND parties)
    _progress("extracting entities with LLM")
    extraction = await _extract_entities(text, matter_id, document_id)

    # 7. Auto-create detected parties
    _progress("creating detected parties")
    detected_parties = extraction.get("parties", [])
    created_party_ids = _create_detected_parties(detected_parties, matter_id, document_id)

    # 8. Update document as processed
    now = datetime.now(timezone.utc).isoformat()
    execute_write(
        f"UPDATE {table('documents')} SET processed_at = :now, page_count = :chunks WHERE document_id = :document_id", {"now": now, "document_id": document_id, "chunks": int(len(chunks))}
    )

    _progress("done")
    return {
        "status": "processed",
        "chunks": len(chunks),
        "issues": len(extraction.get("issues", [])),
        "evidence": len(extraction.get("evidence", [])),
        "timeline_events": len(extraction.get("timeline_events", [])),
        "detected_parties": detected_parties,
        "created_party_ids": created_party_ids,
    }


def _parse_document_from_volume(vol_path: str) -> str:
    """Use ai_parse_document() via SQL to extract text from a file in UC Volume."""
    try:
        # READ_FILES takes a literal path, so only accept files inside this app's documents volume
        prefix = f"/Volumes/{settings.catalog}/{settings.schema}/documents_volume/"
        if not vol_path.startswith(prefix) or not re.fullmatch(r"[A-Za-z0-9._ ()/-]+", vol_path) or ".." in vol_path:
            logger.error("Refusing to parse unexpected path %s", vol_path)
            return ""
        safe_path = vol_path
        sql = f"""
        SELECT to_json(ai_parse_document(content, map('version', '2.0'))) AS parsed_json
        FROM READ_FILES('{safe_path}', format => 'binaryFile')
        """
        rows = execute_query(sql)
        if not rows or not rows[0].get("parsed_json"):
            logger.warning("ai_parse_document returned no results for %s", vol_path)
            return ""

        parsed = json.loads(rows[0]["parsed_json"])

        # Check for errors
        errors = parsed.get("error_status", [])
        if errors:
            logger.warning("ai_parse_document errors: %s", errors)

        # Extract text from document elements
        elements = parsed.get("document", {}).get("elements", [])
        text_parts = []
        for element in elements:
            content = element.get("content")
            if not content:
                continue
            element_type = element.get("type", "")
            if element_type in ("title", "section_header"):
                text_parts.append(f"\n## {content}\n")
            elif element_type == "table":
                text_parts.append(f"\n{content}\n")
            elif element_type in ("page_header", "page_footer", "page_number"):
                continue  # Skip page furniture
            else:
                text_parts.append(content)

        full_text = "\n".join(text_parts).strip()
        logger.info(
            "ai_parse_document extracted %d chars from %d elements (%s)",
            len(full_text),
            len(elements),
            vol_path,
        )
        return full_text

    except Exception as e:
        logger.error("ai_parse_document failed for %s: %s", vol_path, e)
        return ""


def _dbl(value: Any, default: float) -> str:
    """A model-extracted number as exact text for CAST(... AS DOUBLE), clamped to [0, 1]; default if not numeric."""
    try:
        return repr(min(1.0, max(0.0, float(value))))
    except (TypeError, ValueError):
        return repr(default)


def _strength(value: Any) -> str:
    """The model's evidence strength, or 'moderate' if it is not one of the allowed values."""
    v = str(value or "").lower()
    return v if v in {"strong", "moderate", "weak", "circumstantial"} else "moderate"


async def _extract_entities(text: str, matter_id: str, document_id: str) -> dict[str, Any]:
    """Use LLM to extract issues, evidence, timeline, AND parties from document text."""

    rows = execute_query(f"SELECT * FROM {table('matters')} WHERE matter_id = :matter_id", {"matter_id": matter_id})
    matter_context = rows[0].get("domain_context", "") if rows else ""

    max_chars = 30000
    truncated = text[:max_chars] + "..." if len(text) > max_chars else text

    messages = [
        {
            "role": "system",
            "content": """You are an expert analyst extracting structured information from legal and business documents.
Extract the following into a JSON object:
- "parties": array of {name, role, objectives (array of strings), constraints (array of strings), risk_tolerance (0.0-1.0), batna (string), stance ("aggressive"/"neutral"/"cooperative"/"defensive")}
- "issues": array of {title, description, category, importance (0-1)}
- "evidence": array of {title, description, strength ("strong"/"moderate"/"weak"/"circumstantial")}
- "timeline_events": array of {event_date (ISO or descriptive), title, description, significance (0-1)}

For parties:
- Identify ALL distinct parties, entities, organizations, or individuals mentioned as participants in the negotiation, dispute, or deal
- "role" should describe their role (e.g., "Plaintiff", "Defendant", "Buyer", "Seller", "Licensor")
- "objectives" should be inferred goals based on context
- "constraints" should be any limitations, deadlines, or restrictions mentioned
- "risk_tolerance" should be estimated: 0.0=very risk-averse, 1.0=very risk-tolerant
- "batna" should be the best alternative if negotiation fails, inferred from context
- "stance" should be inferred from language and behavior described

Be thorough but concise. Focus on elements relevant to negotiation, dispute, or deal-making.""",
        },
        {
            "role": "user",
            "content": f"Context: {matter_context}\n\nDocument text:\n{truncated}",
        },
    ]

    try:
        result = await chat_completion_json(messages, model=settings.agent_model, max_tokens=8192)
    except Exception as e:
        # Fail the processing run rather than report an empty extraction as a success
        logger.error("LLM extraction failed: %s", e)
        raise RuntimeError(f"Entity extraction failed: {e}") from e

    now = datetime.now(timezone.utc).isoformat()

    # Store extracted issues
    for issue in result.get("issues", []):
        iid = str(uuid.uuid4())
        execute_write(
            f"""INSERT INTO {table('issues')}
            (issue_id, matter_id, title, description, category, affected_parties, importance, created_at)
            VALUES (:iid, :matter_id, :title,
            :description,
            :category, '[]',
            CAST(:importance AS DOUBLE), :now)""", {"iid": iid, "matter_id": matter_id, "title": str(issue.get("title", "")), "description": str(issue.get("description", "")), "category": str(issue.get("category", "")), "now": now, "importance": _dbl(issue.get("importance"), 0.5)}
        )

    # Store extracted evidence
    for ev in result.get("evidence", []):
        eid = str(uuid.uuid4())
        execute_write(
            f"""INSERT INTO {table('evidence_items')}
            (evidence_id, matter_id, title, description, source_document_id, strength, supporting_party_ids, created_at)
            VALUES (:eid, :matter_id, :title,
            :description, :document_id,
            :strength, '[]', :now)""", {"eid": eid, "matter_id": matter_id, "title": str(ev.get("title", "")), "description": str(ev.get("description", "")), "document_id": document_id, "strength": _strength(ev.get("strength")), "now": now}
        )

    # Store extracted timeline events
    for te in result.get("timeline_events", []):
        tid = str(uuid.uuid4())
        execute_write(
            f"""INSERT INTO {table('timeline_events')}
            (event_id, matter_id, event_date, title, description, significance, created_at)
            VALUES (:tid, :matter_id, :event_date,
            :title,
            :description,
            CAST(:significance AS DOUBLE), :now)""", {"tid": tid, "matter_id": matter_id, "event_date": str(te.get("event_date", "")), "title": str(te.get("title", "")), "description": str(te.get("description", "")), "now": now, "significance": _dbl(te.get("significance"), 0.5)}
        )

    # Parties are returned but NOT stored here — process_document handles that
    return result


def _create_detected_parties(
    parties_data: list[dict], matter_id: str, document_id: str
) -> list[str]:
    """Create party records from LLM-detected parties. Returns list of created party_ids."""
    if not parties_data:
        return []

    # Check existing parties to avoid duplicates
    existing = execute_query(
        f"SELECT name FROM {table('parties')} WHERE matter_id = :matter_id", {"matter_id": matter_id}
    )
    existing_names = {r["name"].lower().strip() for r in existing}

    now = datetime.now(timezone.utc).isoformat()
    created_ids = []

    for party in parties_data:
        name = str(party.get("name", "")).strip()
        if not name or name.lower() in existing_names:
            continue

        party_id = str(uuid.uuid4())
        role = str(party.get("role", ""))
        objectives = json.dumps(party.get("objectives", []))
        constraints = json.dumps(party.get("constraints", []))

        risk_tolerance = party.get("risk_tolerance", 0.5)
        try:
            risk_tolerance = max(0.0, min(1.0, float(risk_tolerance)))
        except (TypeError, ValueError):
            risk_tolerance = 0.5

        batna = str(party.get("batna", ""))
        stance = party.get("stance", "neutral")
        if stance not in {"aggressive", "neutral", "cooperative", "defensive"}:
            stance = "neutral"

        execute_write(
            f"""INSERT INTO {table('parties')}
            (party_id, matter_id, name, role, objectives, constraints_, risk_tolerance,
             batna, stance, source, source_document_id, created_at)
            VALUES (:party_id, :matter_id, :name, :role,
            :objectives, :constraints,
            CAST(:risk_tolerance AS DOUBLE), :batna, :stance, 'auto_detected', :document_id, :now)""", {"party_id": party_id, "matter_id": matter_id, "name": name, "role": role, "objectives": objectives, "constraints": constraints, "batna": batna, "stance": stance, "document_id": document_id, "now": now, "risk_tolerance": repr(float(risk_tolerance))}
        )

        created_ids.append(party_id)
        existing_names.add(name.lower())
        logger.info("Created auto-detected party: %s (%s)", name, party_id)

    return created_ids


def _chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    """Split text into overlapping chunks at sentence boundaries."""
    if not text:
        return []

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size

        # Try to break at sentence boundary
        if end < len(text):
            for sep in [". ", ".\n", "! ", "? ", "\n\n"]:
                last_sep = text.rfind(sep, start + chunk_size // 2, end + 100)
                if last_sep != -1:
                    end = last_sep + len(sep)
                    break

        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)

        start = end - overlap
        if start >= len(text):
            break

    return chunks
