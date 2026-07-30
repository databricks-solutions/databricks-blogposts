"""Evaluate the ticket classifier with MLflow 3 GenAI evaluation.

Runs the agent's invoke function over the labelled support tickets with two
scorers: a deterministic exact_match scorer (the promotion gate) and the
out-of-the-box Correctness LLM judge (shown for demonstration). Every prediction
is captured as an MLflow Trace.

Reads the labelled tickets from the Unity Catalog table produced by the data
ingestion job. Prints the exact-match accuracy; the approval job reads that to
decide whether to promote.
"""

import asyncio
import os

import mlflow
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from mlflow.genai.agent_server import get_invoke_function
from mlflow.genai.scorers import Correctness, scorer

load_dotenv(dotenv_path=".env", override=True)

# Import the agent so the @invoke-registered function is discoverable.
from agent_server import agent  # noqa: E402, F401

CATALOG = os.getenv("CATALOG_NAME", "main")
SCHEMA = os.getenv("SCHEMA_NAME", "llmops_quickstart")
ACCURACY_THRESHOLD = float(os.getenv("ACCURACY_THRESHOLD", "0.8"))


def _resolve_warehouse(w: WorkspaceClient) -> str:
    """Use DATABRICKS_WAREHOUSE_ID if set, else the first available SQL warehouse."""
    if os.getenv("DATABRICKS_WAREHOUSE_ID"):
        return os.environ["DATABRICKS_WAREHOUSE_ID"]
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise SystemExit("No SQL warehouse found. Set DATABRICKS_WAREHOUSE_ID.")
    return warehouses[0].id


def _load_eval_dataset() -> list[dict]:
    """Read labelled tickets from the support_tickets table."""
    w = WorkspaceClient()
    resp = w.statement_execution.execute_statement(
        warehouse_id=_resolve_warehouse(w),
        statement=f"SELECT ticket, category FROM {CATALOG}.{SCHEMA}.support_tickets",
        wait_timeout="50s",
    )
    rows = resp.result.data_array or []
    return [
        {
            "inputs": {"data": {"ticket": ticket}},
            "expectations": {"expected_category": category, "expected_response": category},
        }
        for ticket, category in rows
    ]


@scorer
def exact_match(outputs, expectations) -> bool:
    """Predicted category equals the labelled category (case-insensitive)."""
    predicted = str(outputs.get("category", "")).strip().lower()
    expected = str(expectations.get("expected_category", "")).strip().lower()
    return predicted == expected


invoke_fn = get_invoke_function()


def predict_fn(data: dict) -> dict:
    return asyncio.run(invoke_fn(data))


def evaluate():
    assert invoke_fn is not None, (
        "No @invoke-registered function found. Ensure the handler is decorated with @invoke()."
    )

    eval_dataset = _load_eval_dataset()
    print(f"Evaluating on {len(eval_dataset)} labelled tickets.")

    results = mlflow.genai.evaluate(
        data=eval_dataset,
        predict_fn=predict_fn,
        scorers=[exact_match, Correctness()],
    )

    accuracy = float(results.metrics["exact_match/mean"])
    print(f"Exact-match accuracy: {accuracy:.1%}")
    print(f"Threshold: {ACCURACY_THRESHOLD:.0%}")

    if accuracy < ACCURACY_THRESHOLD:
        raise SystemExit(
            f"Accuracy {accuracy:.1%} is below threshold {ACCURACY_THRESHOLD:.0%}. "
            "Improve the agent and re-run."
        )
    print("Accuracy meets the threshold.")
    return accuracy


if __name__ == "__main__":
    evaluate()
