from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    databricks_host: str = field(default_factory=lambda: os.environ.get("DATABRICKS_HOST", ""))
    databricks_token: str = field(default_factory=lambda: os.environ.get("DATABRICKS_TOKEN", ""))
    catalog: str = field(default_factory=lambda: os.environ.get("DATABRICKS_CATALOG", "main"))
    schema: str = field(default_factory=lambda: os.environ.get("DATABRICKS_SCHEMA", "janus"))
    warehouse_id: str = field(default_factory=lambda: os.environ.get("DATABRICKS_WAREHOUSE_ID", ""))
    vs_endpoint_name: str = field(default_factory=lambda: os.environ.get("VS_ENDPOINT_NAME", "janus_vs_endpoint"))

    # Model endpoints
    agent_model: str = field(default_factory=lambda: os.environ.get("AGENT_MODEL", "databricks-claude-sonnet-5-5"))
    judge_model: str = field(default_factory=lambda: os.environ.get("JUDGE_MODEL", "databricks-claude-opus-5-5"))
    embedding_model: str = field(default_factory=lambda: os.environ.get("EMBEDDING_MODEL", "databricks-bge-large-en"))

    def table(self, name: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{name}`"


settings = Settings()
