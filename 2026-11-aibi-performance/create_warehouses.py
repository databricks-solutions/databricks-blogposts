"""Create the two benchmark warehouses: serverless PRO and Lakehouse//RT, both Small with exactly one cluster.

Prints the export lines for BENCH_WAREHOUSE_PRO / BENCH_WAREHOUSE_RT. Both auto-stop after 10 minutes; delete them
when you are done (databricks warehouses delete <id>).

Usage: python create_warehouses.py [--prefix aibi-bench]
"""
import argparse

from databricks.sdk import WorkspaceClient

import config

COMMON = {"cluster_size": "Small", "min_num_clusters": 1, "max_num_clusters": 1, "auto_stop_mins": 10,
          "enable_serverless_compute": True}  # Lakehouse//RT requires serverless as well


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefix", default="aibi-bench")
    a = ap.parse_args()
    w = WorkspaceClient(profile=config.PROFILE)
    pro = w.api_client.do("POST", "/api/2.0/sql/warehouses", body={
        "name": f"{a.prefix}-pro", "warehouse_type": "PRO", **COMMON})
    # Lakehouse//RT (Beta) is warehouse_type REYDEN in the API; other spellings silently create a PRO warehouse
    rt = w.api_client.do("POST", "/api/2.0/sql/warehouses", body={
        "name": f"{a.prefix}-rt", "warehouse_type": "REYDEN", **COMMON})
    print(f"export BENCH_WAREHOUSE_PRO={pro['id']}")
    print(f"export BENCH_WAREHOUSE_RT={rt['id']}")


if __name__ == "__main__":
    main()
