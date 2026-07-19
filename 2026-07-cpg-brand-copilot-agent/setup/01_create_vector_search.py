# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 01 — Vector Search endpoint + Delta-synced index
# MAGIC Creates the `VS_ENDPOINT` (STANDARD) and a Delta-sync index over the `documents` table so
# MAGIC the agent can do RAG retrieval. One vector per doc (managed embeddings over `content`);
# MAGIC the other columns sync as retrievable metadata.
# MAGIC
# MAGIC In-workspace rewrite of the old `setup/01_create_vector_search.sh` — same REST calls, no
# MAGIC local CLI / profile. All values come from `../config`.
# MAGIC
# MAGIC **Prereqs:** data generation complete (the `documents` table exists with Change Data Feed).

# COMMAND ----------
# MAGIC %run ../config

# COMMAND ----------
import time

# COMMAND ----------
# 1. Create the Vector Search endpoint (STANDARD). Idempotent.
try:
    w.api_client.do(
        "POST", "/api/2.0/vector-search/endpoints",
        body={"name": VS_ENDPOINT, "endpoint_type": "STANDARD"},
    )
    print("Endpoint create requested:", VS_ENDPOINT)
except Exception as e:
    print("Endpoint create (may already exist):", e)

# Wait for the endpoint to be ONLINE (fresh STANDARD endpoints can take ~10-20 min).
print("Waiting for endpoint to be ONLINE ...")
while True:
    ep = w.api_client.do("GET", f"/api/2.0/vector-search/endpoints/{VS_ENDPOINT}")
    state = (ep.get("endpoint_status") or {}).get("state", "UNKNOWN")
    if state == "ONLINE":
        print("   endpoint ONLINE.")
        break
    print("   ...", state)
    time.sleep(30)

# COMMAND ----------
# 2. Create the Delta-synced index over the documents table. Idempotent.
index_spec = {
    "name": VS_INDEX,
    "endpoint_name": VS_ENDPOINT,
    "primary_key": "doc_id",
    "index_type": "DELTA_SYNC",
    "delta_sync_index_spec": {
        "source_table": DOCS_TABLE,
        "pipeline_type": "TRIGGERED",
        "embedding_source_columns": [
            {"name": "content", "embedding_model_endpoint_name": EMBEDDING_ENDPOINT}
        ],
    },
}
try:
    w.api_client.do("POST", "/api/2.0/vector-search/indexes", body=index_spec)
    print("Index create requested:", VS_INDEX)
except Exception as e:
    print("Index create (may already exist):", e)

# COMMAND ----------
# 3. Wait until the index is READY (initial sync + embedding can take several minutes).
print("Waiting for index to be READY ...")
while True:
    idx = w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{VS_INDEX}")
    status = idx.get("status") or {}
    if status.get("ready"):
        print("   index READY.")
        break
    print("   ...", status.get("detailed_state") or status.get("index_state") or "provisioning/syncing")
    time.sleep(30)

# COMMAND ----------
# 4. Validate with a similarity query.
query = {
    "query_text": "what allergens are in Aurora oat milk and what do customers dislike about it",
    "columns": ["doc_id", "doc_type", "title", "content"],
    "num_results": 4,
}
res = w.api_client.do("POST", f"/api/2.0/vector-search/indexes/{VS_INDEX}/query", body=query)
data = (res.get("result") or {}).get("data_array") or []
print(f"hits: {len(data)}")
for r in data:
    print(f"  [{r[1]}] {r[2]}  (score={round(float(r[-1]), 3)})")

print(f"\nDONE. Index {VS_INDEX} is ready on endpoint {VS_ENDPOINT}.")
print(f"MCP URL for the agent: {HOST}/api/2.0/mcp/vector-search/{CATALOG}/{SCHEMA}")
print("\nNext: run setup/02_create_genie_space")
