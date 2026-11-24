# MV incremental refresh demo

Runs a "first and latest event per shipment" materialized view two ways in one Lakeflow Spark
Declarative Pipeline:

- **the naive MV**, with `ROW_NUMBER()` windows: a full recompute on every refresh;
- **the refactored chain**, with change data feed + AUTO CDC + staged MVs: refreshes incrementally.

It also compares the refresh technique of each MV from the pipeline event log.

| What | Value |
|---|---|
| Workspace / CLI profile | `e2-demo-field-eng` |
| Catalog.schema | `roger_ding_catalog.mv_incremental_refresh` |
| Landing volume | `/Volumes/roger_ding_catalog/mv_incremental_refresh/landing/raw/{legs,events}/` |
| Pipeline | `mv_incremental_refresh` (bundle key `shipments_pipeline`) |
| SQL warehouse | your own, used by `validate.py` and `evidence.py` |

## What the pipeline builds

```
raw JSON (volume)
  src/bronze.py   Auto Loader    → shipment_legs_raw, tracking_events_raw        (append-only)
  src/silver.py   AUTO CDC SCD1  → shipment_legs, tracking_events                 (insert + update)
                  shipment_legs CDF + AUTO CDC → shipments
  src/shipment_summary_naive.sql → shipment_summary_naive    (windows + joins in one MV: the "before")
  src/shipment_summary.py
                  tracking_events CDF → event_changes (temporary view)
                  AUTO CDC SCD1  → shipment_first_event, shipment_latest_event   (one row per shipment)
                  MVs            → latest_enriched, first_enriched → shipment_summary   (≤ 2 joins each)
                  MV             → shipment_summary_flat     (no windows, 4 joins: for comparison)
```

## Files

| File | What |
|---|---|
| `databricks.yml`, `resources/shipments_pipeline.yml` | The bundle and the pipeline definition |
| `src/` | Pipeline source (see above) |
| `setup.sh` | One-time: creates the schema and the landing volume |
| `gen_batches.py` | Writes the 4 scenario batches to `data/batch_<n>/{legs,events}/` (shipments S1–S7) |
| `run_batch.sh` | Uploads one batch, runs a pipeline update, runs `validate.py`, saves to `results/batch_<n>.txt` |
| `validate.py` | Diff of `shipment_summary` vs `shipment_summary_naive`, each MV's refresh technique and rejection reasons, rows per flow, snapshots |
| `evidence.py` | More read-only checks: refresh history, update causes, change feeds, commit history |
| `data/` | The 4 input batches |
| `runbook.md` | How to run the demo, step by step, with the expected result of every check |
| `results/` | Output of a full run |

## What it shows

| MV | Batch 1 | Batches 2–4 |
|---|---|---|
| `latest_enriched`, `first_enriched`, `shipment_summary` | `COMPLETE_RECOMPUTE` (`FIRST_COMPUTATION`) | `ROW_BASED`, incremental (`latest_enriched` is `APPEND_ONLY` in batch 3) |
| `shipment_summary_naive`, `shipment_summary_flat` | `COMPLETE_RECOMPUTE` (`FIRST_COMPUTATION`) | `COMPLETE_RECOMPUTE`, `NUM_JOINS_THRESHOLD_EXCEEDED` |

`shipment_summary` and `shipment_summary_naive` hold the same rows after every batch.

## Run it

See [`runbook.md`](runbook.md): setup, running the batches, the expected result of every check,
rerunning from scratch, and troubleshooting.
