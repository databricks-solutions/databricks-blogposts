# Databricks notebook source
# MAGIC %md
# MAGIC # Setup 03 — Create the Lakebase (Postgres) instance
# MAGIC Provisions the managed Postgres instance `LAKEBASE_INSTANCE` that backs the agent's
# MAGIC long-term memory. New in-workspace step (previously a local CLI / UI action).
# MAGIC Idempotent, and waits until the instance is `AVAILABLE`. All values from `../config`.

# COMMAND ----------
# MAGIC %run ../config

# COMMAND ----------
import time

# COMMAND ----------
# Resolve-or-create the instance.
try:
    inst = w.api_client.do("GET", f"/api/2.0/database/instances/{LAKEBASE_INSTANCE}")
    print("Instance already exists:", LAKEBASE_INSTANCE, "| state:", inst.get("state"))
except Exception:
    print("Creating Lakebase instance:", LAKEBASE_INSTANCE, f"({LAKEBASE_CAPACITY})")
    w.api_client.do(
        "POST", "/api/2.0/database/instances",
        body={"name": LAKEBASE_INSTANCE, "capacity": LAKEBASE_CAPACITY},
    )

# COMMAND ----------
# Wait until AVAILABLE (provisioning takes a few minutes).
print("Waiting for instance to be AVAILABLE ...")
while True:
    inst = w.api_client.do("GET", f"/api/2.0/database/instances/{LAKEBASE_INSTANCE}")
    state = inst.get("state", "UNKNOWN")
    if state == "AVAILABLE":
        print("   AVAILABLE.")
        break
    if state in ("FAILED", "DELETING", "STOPPED"):
        raise RuntimeError(f"Instance is in unexpected state: {state}")
    print("   ...", state)
    time.sleep(30)

print("read_write_dns:", inst.get("read_write_dns"))
print("\nNext: run setup/04_lakebase_schema")
