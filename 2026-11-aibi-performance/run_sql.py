"""Run a .sql file (statements separated by ';' at line end) on a SQL warehouse.

${catalog} / ${schema} are replaced from the config, ${nonce} with the current time (used by the cache invalidation).

Usage: python run_sql.py <file.sql> [--warehouse setup|pro|rt|<id>]
"""
import argparse
import re
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

import config


def split_statements(text: str) -> list[str]:
    # strip full-line comments, split on ';' at end of line (dollar-quoted YAML has no trailing ';')
    lines = [l for l in text.splitlines() if not l.strip().startswith("--")]
    parts = re.split(r";\s*\n", "\n".join(lines) + "\n")
    return [p.strip() for p in parts if p.strip()]


def run(w: WorkspaceClient, wh: str, sql: str) -> None:
    t0 = time.time()
    resp = w.statement_execution.execute_statement(statement=sql, warehouse_id=wh, wait_timeout="50s")
    sid = resp.statement_id
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(3)
        resp = w.statement_execution.get_statement(sid)
    head = " ".join(sql.split())[:110]
    if resp.status.state != StatementState.SUCCEEDED:
        print(f"FAILED ({time.time()-t0:.1f}s): {head}\n  {resp.status.error.message}", flush=True)
        sys.exit(1)
    print(f"OK ({time.time()-t0:.1f}s): {head}", flush=True)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--warehouse", default="setup")
    a = ap.parse_args()
    w = WorkspaceClient(profile=config.PROFILE)
    wh = config.warehouse(a.warehouse)
    text = config.substitute(open(a.file).read(), nonce=int(time.time()))
    for stmt in split_statements(text):
        run(w, wh, stmt)
