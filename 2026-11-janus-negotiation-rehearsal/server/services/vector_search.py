from __future__ import annotations

import json

import logging
from typing import Any

from server.config import settings
from server.services.databricks_client import get_workspace_client
from server.services.foundation_model import generate_embeddings

logger = logging.getLogger(__name__)


async def search_chunks(
    query_text: str,
    matter_id: str,
    num_results: int = 5,
) -> list[dict[str, Any]]:
    """Search document chunks using Vector Search index.

    Falls back to embedding similarity if VS index is not available.
    """
    try:
        return await _vector_search_query(query_text, matter_id, num_results)
    except Exception as e:
        logger.warning(f"Vector Search failed, falling back to in-memory: {e}")
        return await _fallback_search(query_text, matter_id, num_results)


async def _vector_search_query(
    query_text: str,
    matter_id: str,
    num_results: int,
) -> list[dict[str, Any]]:
    """Query using Databricks Vector Search index."""

    client = get_workspace_client()
    index_name = f"{settings.catalog}.{settings.schema}.document_chunks_index"

    query_embedding = await generate_embeddings([query_text])
    if not query_embedding or not query_embedding[0]:
        return []

    results = client.vector_search_indexes.query_index(
        index_name=index_name,
        columns=["chunk_id", "document_id", "matter_id", "text", "chunk_index"],
        query_vector=query_embedding[0],
        num_results=num_results,
        filters_json=json.dumps({"matter_id": [matter_id]}),
    )

    return [
        {
            "chunk_id": row[0],
            "document_id": row[1],
            "matter_id": row[2],
            "text": row[3],
            "chunk_index": row[4],
            "score": row[-1] if len(row) > 5 else 0.0,
        }
        for row in results.get("result", {}).get("data_array", [])
    ]


async def _fallback_search(
    query_text: str,
    matter_id: str,
    num_results: int,
) -> list[dict[str, Any]]:
    """Fallback: load chunks from DB and compute cosine similarity in-memory."""
    import numpy as np

    from server.services.databricks_client import execute_query, table

    # Get query embedding
    query_emb = await generate_embeddings([query_text])
    if not query_emb or not query_emb[0]:
        # No embeddings available, return text-match fallback
        rows = execute_query(
            f"SELECT chunk_id, document_id, matter_id, text, chunk_index FROM {table('document_chunks')} "
            f"WHERE matter_id = :matter_id LIMIT {int(num_results)}",
            {"matter_id": matter_id},
        )
        return rows

    q = np.array(query_emb[0])
    q_norm = np.linalg.norm(q)
    if q_norm == 0:
        return []

    rows = execute_query(
        f"SELECT chunk_id, document_id, matter_id, text, chunk_index, embedding "
        f"FROM {table('document_chunks')} WHERE matter_id = :matter_id",
        {"matter_id": matter_id},
    )

    scored = []
    for row in rows:
        emb = row.get("embedding")
        if not emb:
            continue
        e = np.array(emb)
        e_norm = np.linalg.norm(e)
        if e_norm == 0:
            continue
        sim = float(np.dot(q, e) / (q_norm * e_norm))
        scored.append({**row, "score": sim})

    scored.sort(key=lambda x: x["score"], reverse=True)
    # Remove embedding from results
    for item in scored:
        item.pop("embedding", None)
    return scored[:num_results]
