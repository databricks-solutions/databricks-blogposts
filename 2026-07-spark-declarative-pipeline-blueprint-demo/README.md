
***

# Workflow Test Automation Blueprint on Databricks

This repository demonstrates best practices for automated testing of Databricks jobs within a CI/CD pipeline, following our multi-environment process as illustrated in the included workflow diagram.

***

### Workflow Overview

![testing_flow.png](img/testing_flow.png)

- **Feature Development**
    - Developers work on feature branches, running unit/component tests locally and optionally integration tests against the Dev Databricks workspace.
- **Pull Request**
    - Opening a PR to main branch triggers CI/CD agent to run automated unit/component tests.
    - Results are returned, and status checks decide if merging is possible.
- **Main Branch**
    - On merging to main, the CI/CD agent runs unit/component/integration tests against Test Databricks workspace.
- **Release**
    - Acceptance tests are executed in Acceptance workspace when release branches/tags are created.
    - Successful runs trigger deployments to Prod workspace.

***

### Testing Approaches

**Unit and Component Testing**

- Run on local laptop for rapid feedback.
- Triggered automatically by CI/CD agent on PR open/merge.

**Integration Testing**

- Executed against a Test workspace after merge to main.
- Involves setting up environment, running jobs, validating outputs, and cleaning up.

**Acceptance Testing**

- Executed in the Acceptance workspace after release branch/tag creation.
- Results from acceptance tests determine if code is ready for production.

***

### Integration Testing Details

- Set up required environment (clusters, databases, etc.).
- Run job using predefined input data.
- Validate output against expected results for consistency.
- Clean up resources once testing is complete.

**Approach 1: Dedicated Integration Workflow**

- Deploy separate workflow for integration tests (IT).
- Ensure input data is consistent between runs.
- Use different target databases identified, for example, by Run ID.
- Optionally combine multiple itegration tests in a single workflow for parallelization, shared clusters, and run repairs.

**Approach 2: Pytest-Based Validation**

- Use pytest to orchestrate integration validation:
    - Set up the Databricks environment from local machine or CI/CD agent.
    - Run the main job on Databricks workspace.
    - Validate results using pytest assertions.

This repo showcase approach two. It uses 
- [Pytester](https://github.com/databrickslabs/pytester), a Databricks labs project, to setup and cleanup the test environment on Databricks per run.
- [Databricks Connect](https://docs.databricks.com/aws/en/dev-tools/databricks-connect/python/) to execture spark code locally on Databricks cluster
- [Databricks SDK](https://docs.databricks.com/aws/en/dev-tools/sdk-python) to execute jobs remotely

***

### Example Code and Instructions

This code repository comes with an example job and sample unit and integration tests to showcase the apporach of running integration tests on Databricks using pytest for 

- Environmment setup
- Assertion and validation

#### How to Run

##### Setting up the environment

- [Setup Databricks CLI locally](https://docs.databricks.com/aws/en/dev-tools/cli/install)
- [Authenticate the CLI to your dev workspace](https://docs.databricks.com/aws/en/dev-tools/cli/authentication)
- [Install Python `uv`](https://docs.astral.sh/uv/guides/install-python/)
- Setup the python project using `uv`
    - `uv venv`
    - `uv sync`
- Update `databricks.yml` with the appropriate workspace configuration
- Deploy the bundle to the dev workspace

```
databricks bundle deploy -t dev
```
##### Running Unit Tests

Run the following commands

```
uv sync --only-group unit-tests
```

```
uv run python -m pytest -m unit_test
```

##### Running integration Tests

Setup environment variables for how to connec to the databricks workspace and clusters

```
export DATABRICKS_HOST=<your-dev-workspace-url>
export DATABRICKS_CLUSTER_ID=<cluster-id> # to be used to run spark code on Databricks
export DATABRICKS_WAREHOUSE_ID= # set it as empty

# optionally if you want to use serverless instead of classic compute, set this instead of DATABRICKS_CLUSTER_ID
export DATABRICKS_SERVERLESS_COMPUTE_ID=auto
```

Run the following commands

```
uv sync --only-group integration-tests
```

```
uv run python -m pytest -rsx -m integration_test --job-name="<job-name-to-test>"
```

##### Running SDP Pipeline Integration Tests

The repository also includes an integration test for a Lakeflow Spark Declarative Pipeline (SDP) with streaming tables. The test:

1. Looks up the deployed pipeline by name and reads its spec.
2. Creates an ephemeral schema for isolation (via `make_schema`).
3. Creates a fresh, isolated pipeline instance targeting the ephemeral schema.
4. Triggers a `full_refresh` update (clears all streaming checkpoints and table data) and waits for completion.
5. Verifies the update status is `COMPLETED` (not just `IDLE`) and validates that both the streaming table and materialized view contain data.
6. Cleans up the test pipeline in a `finally` block.

Deploy the pipeline and run the test:

```
databricks bundle deploy -t dev
```

```
uv run python -m pytest -rsx -m integration_test --pipeline-name="<pipeline-name-to-test>"
```

You can run both job and pipeline tests together:

```
uv run python -m pytest -rsx -m integration_test \
  --job-name="<job-name-to-test>" \
  --pipeline-name="<pipeline-name-to-test>"
```


***


