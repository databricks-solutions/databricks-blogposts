# Runbook: MV incremental refresh demo

Step-by-step instructions to run the demo and check its results. Run every command from the
repository root.

| What | Value (change the profile and catalog to yours, see [section 0](#use-your-own-workspace-profile-and-catalog)) |
|---|---|
| Workspace / CLI profile | `e2-demo-field-eng` |
| Catalog.schema | `roger_ding_catalog.mv_incremental_refresh` |
| Landing volume | `/Volumes/roger_ding_catalog/mv_incremental_refresh/landing/raw/{legs,events}/` |
| Pipeline | `mv_incremental_refresh` (bundle key `shipments_pipeline`), serverless |
| SQL warehouse | any SQL warehouse you can use; `validate.py` and `evidence.py` run their queries on it |

The demo feeds **4 batches** of synthetic shipment data through the pipeline, **one pipeline update
per batch**. After each batch it checks two things:

- **Correctness:** `shipment_summary` (refactored) gives the same result as
  `shipment_summary_naive` (the original window-based MV).
- **Incremental refresh:** which refresh technique each MV chose, from the pipeline event log.

---

## 0. Prerequisites

- **Databricks CLI** with bundle support (tested with v1.18.0): `databricks --version`.
- **Python 3**. No extra packages; the scripts call the CLI.
- **Permissions:** create a schema and a volume in your catalog, run serverless pipelines, and use
  a SQL warehouse.

### Use your own workspace, profile and catalog

The repo is set up for the author's environment: CLI profile `e2-demo-field-eng`, workspace
`https://e2-demo-field-eng.cloud.databricks.com/`, catalog `roger_ding_catalog`. **Before you run
anything, change them to your own.** They're hard-coded in these places:

| File | Line | Change to |
|---|---|---|
| `databricks.yml` | `host: https://e2-demo-field-eng.cloud.databricks.com/` | your workspace URL |
| `databricks.yml` | `catalog: default: roger_ding_catalog` | your catalog |
| `setup.sh` | `PROFILE=${PROFILE:-e2-demo-field-eng}` | your CLI profile (or `export PROFILE=...`) |
| `setup.sh` | `roger_ding_catalog` (2×, schema and volume) | your catalog |
| `run_batch.sh` | `PROFILE=${PROFILE:-e2-demo-field-eng}` | your CLI profile (or `export PROFILE=...`) |
| `run_batch.sh` | `VOL=dbfs:/Volumes/roger_ding_catalog/...` | your catalog |
| `validate.py` | `os.environ.get("PROFILE", "e2-demo-field-eng")` | your CLI profile (or `export PROFILE=...`) |
| `validate.py` | `os.environ.get("FQ", "roger_ding_catalog.mv_incremental_refresh")` | your catalog (or `export FQ=<catalog>.mv_incremental_refresh`) |

`evidence.py` reuses `validate.py`'s settings. The pipeline gets its catalog and schema from
`databricks.yml`, so nothing under `src/` needs to change.

A quick way to find every occurrence:

```bash
grep -rn -e e2-demo-field-eng -e roger_ding_catalog --exclude-dir=.git --exclude-dir=.databricks .
```

In the rest of this runbook, replace `e2-demo-field-eng` and `roger_ding_catalog` with your
profile and catalog wherever they appear in a command.

### Set up the shell

```bash
export PROFILE=<your CLI profile>                        # the author uses e2-demo-field-eng
databricks auth login -p $PROFILE                       # re-run whenever you see "invalid_client"
databricks current-user me -p $PROFILE | grep userName
```

Pick a SQL warehouse and export its ID:

```bash
databricks warehouses list -p $PROFILE
export WAREHOUSE_ID=<warehouse id>
```

If you don't have one, create a small one. It stops itself after 10 idle minutes:

```bash
databricks warehouses create -p $PROFILE --no-wait --json '{"name":"mv-incremental-refresh",
  "cluster_size":"2X-Small","min_num_clusters":1,"max_num_clusters":1,"auto_stop_mins":10,
  "enable_serverless_compute":true,"warehouse_type":"PRO"}' -o json | grep '"id"'
```

---

## 1. Create the schema and the landing volume (once)

```bash
./setup.sh
```

This creates the schema `roger_ding_catalog.mv_incremental_refresh` and the managed volume
`landing` in it. If either already exists, the command fails; that's fine if they're left over
from a previous run (see [section 6](#6-rerun-from-scratch) for a clean rerun).

---

## 2. Input data

The 4 batches are already in `data/`:

```
data/batch_<n>/legs/legs_batch_<n>.json       shipment legs: one row per leg version
data/batch_<n>/events/events_batch_<n>.json   tracking events: one row per event version
```

To regenerate them, run `python3 gen_batches.py`. Each batch exercises specific cases; the
shipment ID tells you which:

| Shipment | What it tests |
|---|---|
| S1 | a newer event (batch 2), then a late *earlier* event (batch 3): the first event moves |
| S2 | two legs: leg B with a newer event (b2), then a late earlier event on leg B (b3) |
| S3 | the latest event is corrected in place (b2); a newer event (b4) |
| S4 | leg-only update: the consignee account changes, no new event (b2) |
| S5 | leg-only update (b4) |
| S6 | the event arrives before its leg (b1); the leg lands in b2 |
| S7 | a brand-new shipment (b3) |

---

## 3. Deploy the pipeline

```bash
databricks bundle validate -p $PROFILE
databricks bundle deploy   -p $PROFILE          # "Created pipelines.shipments_pipeline"
```

Don't run the pipeline yet. With an empty landing folder, it would only create empty tables.

---

## 4. Run the batches

Run them **in order, one at a time**. Each batch must be its own pipeline update for the scenarios
to mean anything.

```bash
./run_batch.sh 1
./run_batch.sh 2
./run_batch.sh 3
./run_batch.sh 4
```

`run_batch.sh <n>` does three things:

1. uploads `data/batch_<n>/` to the landing volume;
2. runs one pipeline update and waits for it to finish (about 2 minutes);
3. runs `validate.py`, prints its report and saves it to `results/batch_<n>.txt`. Set
   `RESULTS_DIR` to save elsewhere.

`validate.py` prints these sections:

| Section | What it shows |
|---|---|
| `DIFF shipment_summary vs shipment_summary_naive` | rows that differ between the two; empty means identical |
| `MV refresh techniques` | for each MV's latest refresh: every candidate technique, which one was chosen, and why the others were rejected |
| `Rows processed per flow` | rows each table processed in the latest update |
| `shipment_summary`, `shipment_first_event`, `shipment_latest_event`, `shipments` | snapshots |

After batch 4, collect the extra evidence:

```bash
python3 evidence.py | tee results/evidence_after_batch_4.txt
```

---

## 5. Verify the results

### 5.1 Refresh techniques

From the `MV refresh techniques` section, the row with `chosen = true` for each MV:

| MV | Batch 1 | Batch 2 | Batch 3 | Batch 4 |
|---|---|---|---|---|
| `latest_enriched` | `COMPLETE_RECOMPUTE` | `ROW_BASED` | `APPEND_ONLY` | `ROW_BASED` |
| `first_enriched` | `COMPLETE_RECOMPUTE` | `ROW_BASED` | `ROW_BASED` | `ROW_BASED` |
| `shipment_summary` | `COMPLETE_RECOMPUTE` | `ROW_BASED` | `ROW_BASED` | `ROW_BASED` |
| `shipment_summary_naive` | `COMPLETE_RECOMPUTE` | `COMPLETE_RECOMPUTE` | `COMPLETE_RECOMPUTE` | `COMPLETE_RECOMPUTE` |
| `shipment_summary_flat` | `COMPLETE_RECOMPUTE` | `COMPLETE_RECOMPUTE` | `COMPLETE_RECOMPUTE` | `COMPLETE_RECOMPUTE` |

✅ **Pass:**
- **Batch 1:** every MV's rejected incremental plan says `FIRST_COMPUTATION`.
- **Batches 2–4:**
  - the three refactored MVs choose an incremental technique (`ROW_BASED`, or `APPEND_ONLY`
    when the inputs only got inserts);
  - the naive and flat MVs do a full recompute, and their rejected plan row says
    `INCREMENTAL_PLAN_REJECTED_BY_COST_MODEL / NUM_JOINS_THRESHOLD_EXCEEDED`.

How to read the section:

- `COMPLETE_RECOMPUTE` appears as a candidate on **every** refresh. Only `chosen = true` is what ran.
- A row with an **empty technique** and a reason is an incremental plan that was built and then
  rejected. That reason is the one that matters.
- `DATA_HAS_CHANGED` only rejects `NO_OP` ("there's new data"). It's normal.
- Incremental techniques never contain the word "INCREMENTAL". Anything other than
  `COMPLETE_RECOMPUTE` and `NO_OP` is incremental.

### 5.2 Correctness

From the `DIFF` section:

✅ **Pass:** **0 rows after every batch.** `shipment_summary` and `shipment_summary_naive` hold
exactly the same rows.

### 5.3 `shipment_summary` after each batch

| Shipment | Batch 1 | Batch 2 | Batch 3 | Batch 4 |
|---|---|---|---|---|
| S1 | Picked up | **Delivered** | Delivered | Delivered |
| S2 | Hub A, In transit, BK2A | **Hub B, Delivered**, BK2A | **BK2B**, ship date 2026-10-02 | — |
| S3 | In transit | **Out for delivery** | Out for delivery | **Delivered** |
| S4 | ACC4-OLD | **ACC4-NEW** | — | — |
| S5 | Picked up, ACC5 | — | — | **ACC5-NEW** |
| S6 | absent | **Picked up, BK6, ACC6** | present | present |
| S7 | — | — | **Picked up, BK7** | present |
| **rows** | **5** | **6** | **7** | **7** |

"—" means no change.

### 5.4 Rows processed per flow

The `Rows processed per flow` section shows each streaming table touching only the new rows. For
example, in batch 2 `shipment_legs` upserts 3 rows and `tracking_events` 3. For the MVs,
`output_rows` is the size of the result, not the number of rows changed. Use 5.1, not this
section, to tell whether an MV refreshed incrementally.

### 5.5 Extra evidence (`evidence.py`)

| Section | Expected |
|---|---|
| `Q1 history` | one row per MV refresh: 4 refreshes per MV, matching 5.1 |
| `Q3 summary across refreshes` | the naive and flat MVs: 3 refreshes rejected with `NUM_JOINS_THRESHOLD_EXCEEDED`, plus 1 `FIRST_COMPUTATION` |
| `create_update` | 4 updates, all `full_refresh = false`, cause `API_CALL` |
| `shipments flow: upserted rows per update` | 5, 3, 1, 1 |
| `DESCRIBE HISTORY shipment_legs` | MERGEs with 5 / 2+1 / 1 / 0+1 rows inserted + updated (version numbers may vary) |
| `CDF of tracking_events for S3` | 2 inserts, then the in-place correction as `update_preimage` / `update_postimage`, then the batch-4 insert |
| `S6 presence (event before its leg)` | 1 row in every table: `shipment_legs`, `shipments`, `shipment_latest_event`, `shipment_summary`, `shipment_summary_naive` |

---

## 6. Rerun from scratch

Auto Loader ingests each file only once, and the streaming tables keep checkpoints, so rerunning the
batches on the same pipeline does nothing. To start over, delete the pipeline and the whole schema,
then recreate them:

```bash
databricks bundle destroy -p $PROFILE --auto-approve                              # the pipeline

# Check the schema only holds this demo's objects, then drop it with everything in it:
databricks tables list roger_ding_catalog mv_incremental_refresh -p $PROFILE -o json \
  | python3 -c "import json,sys; [print(t['table_type'], t['name']) for t in json.load(sys.stdin)]"
databricks schemas delete roger_ding_catalog.mv_incremental_refresh --force -p $PROFILE

./setup.sh                                                                        # schema + volume again
```

Then continue from [section 3](#3-deploy-the-pipeline).

`--force` matters. A pipeline's MVs and streaming tables are backed by hidden internal tables
(`__materialization_mat_<pipeline id>_...`). They don't appear in `tables list`, aren't always
removed with the pipeline, and make a plain `schemas delete` fail with "Schema ... is not empty"
even after every visible table is gone. `--force` removes them, together with the tables, the
volume and the uploaded files.

## 7. Clean up

Delete the pipeline and the schema as in section 6, without running `setup.sh` again.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `invalid_client` / token refresh failed | CLI login expired | `databricks auth login -p $PROFILE` |
| `set WAREHOUSE_ID` | Env var missing | `export WAREHOUSE_ID=<id>` |
| `The warehouse ... was not found` | Wrong or deleted warehouse | Pick another one (section 0) |
| `setup.sh`: schema already exists | Previous run | Section 6 for a clean start (`setup.sh` stops at the first error, so the volume may be missing) |
| `schemas delete`: "Schema ... is not empty", but `tables list` is empty | Hidden backing tables of a deleted pipeline | Add `--force` (section 6) |
| `Table ... is already managed by pipeline <id>` | Tables left by a destroyed pipeline | Section 6 |
| A batch "did nothing" (0 rows processed) | Those files were already ingested | Section 6, then rerun all batches in order |
| Results don't match section 5 | Batches run out of order, or two batches in one update | Section 6, then one `run_batch.sh` per batch, in order |
| A run failed right after a fresh deploy, and later runs misbehave | The failed update left partial stream checkpoints | `databricks bundle run shipments_pipeline -p $PROFILE --full-refresh-all` |
