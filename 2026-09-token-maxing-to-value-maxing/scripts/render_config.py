"""Render app.yaml env + bundle variables from a single customer.yaml.

  python3 scripts/render_config.py --config customer.yaml            # writes app/app.yaml
  python3 scripts/render_config.py --config customer.yaml --print-vars   # prints `--var k=v …` for the bundle

Keeps `customer.yaml` as the single source of truth: the app's runtime env is
generated here, and the same values feed the Databricks Asset Bundle. Requires
PyYAML (`pip install pyyaml`).
"""
import argparse, json, os, sys
import yaml

ROOT = os.path.join(os.path.dirname(__file__), "..")


def resolve_pricing(c):
    """Build the canonical full per-model pricing map used by BOTH the app and the
    loader. Base = the region-resolved map from fetch_pricing (pricing.resolved.json)
    if present, else the bundled fallback (model_pricing.json). `customer.yaml`'s
    optional `model_pricing` block is merged on top as a manual override (UI labels,
    per-model rates, or the legacy expensive/cheap tiers)."""
    base = None
    for fn in ("pricing.resolved.json", "model_pricing.json"):
        p = os.path.join(ROOT, "scripts", "gateway_etl", fn)
        if os.path.exists(p):
            base = json.load(open(p))
            break
    base = base or {"models": {}, "default": {"input": 1.0, "output": 5.0}, "ui": {}}
    models = dict(base.get("models") or {})
    default = dict(base.get("default") or {"input": 1.0, "output": 5.0})
    ui = dict(base.get("ui") or {})

    ov = c.get("model_pricing") or {}
    labels = ov.get("labels") or {}
    if labels.get("expensive"):
        ui["expensive_label"] = labels["expensive"]
    if labels.get("cheap"):
        ui["cheap_label"] = labels["cheap"]
    # Legacy expensive/cheap tier shape → labels + a model entry each.
    for tk, tier in (("expensive", "premium"), ("cheap", "standard")):
        t = ov.get(tk)
        if isinstance(t, dict):
            if t.get("label"):
                ui[f"{tk}_label"] = t["label"]
            if t.get("name"):
                models[t["name"]] = {"input": t.get("input", 1.0), "output": t.get("output", 5.0), "tier": tier}
    # Explicit per-model overrides win.
    for nm, r in (ov.get("models") or {}).items():
        models[nm] = {**models.get(nm, {}), **r}
    if isinstance(ov.get("default"), dict):
        default.update(ov["default"])
    ui.setdefault("expensive_label", "Premium")
    ui.setdefault("cheap_label", "Standard")
    return {"models": models, "default": default, "ui": ui}


def render_app_yaml(c):
    mp = json.dumps(resolve_pricing(c), separators=(",", ":"))
    env = {
        "PGHOST": c["lakebase"]["host"], "PGDATABASE": c["lakebase"]["database"],
        "PGPORT": "5432", "PGSSLMODE": "require", "APP_SP_ROLE": c["lakebase"]["app_sp"],
        "APP_URL": c["app"]["url"], "EMAIL_DOMAIN": c["identity"]["email_domain"],
        "ADMIN_EMAILS": ",".join(c["identity"].get("admins", [])),
        "MANAGER_EMAILS": ",".join(c["identity"].get("managers", [])),
        "IDENTITY_SOURCE": "directory",
        "SOURCE_INFERENCE_TABLE": c["sources"]["inference_table"],
        "SOURCE_DIRECTORY_TABLE": c["sources"].get("directory_table", ""),
        "SOURCE_USAGE_TABLE": c["sources"].get("usage_table", "system.serving.endpoint_usage"),
        "UC_CATALOG": c["uc"]["catalog"], "UC_SCHEMA": c["uc"]["schema"],
        "CLASSIFIER_MODEL": c["app"].get("classifier_model", "databricks-claude-haiku-4-5"),
        "MODEL_PRICING": mp,
        "MAIL_FROM_NAME": c["mail"].get("from_name", "GatewayIQ"),
        "MAIL_FROM_EMAIL": c["mail"].get("from_email", ""),
        "GOOGLE_QUOTA_PROJECT": c["mail"].get("quota_project", ""),
    }
    lines = ['# GENERATED from customer.yaml by render_config.py — do not edit by hand.',
             'command: ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]', "", "env:"]
    for k, v in env.items():
        lines.append(f'  - name: "{k}"')
        lines.append(f'    value: {json.dumps(v)}')
    # Gmail creds come from the secret-scope app resources (defined in the bundle).
    for name, key in [("GMAIL_CLIENT_ID", "gmail-client-id"), ("GMAIL_CLIENT_SECRET", "gmail-client-secret"),
                      ("GMAIL_REFRESH_TOKEN", "gmail-refresh-token")]:
        lines.append(f'  - name: "{name}"')
        lines.append(f'    valueFrom: "{key}"')
    open(os.path.join(ROOT, "app", "app.yaml"), "w").write("\n".join(lines) + "\n")


def bundle_vars(c):
    wr = c["weekly_report"]
    dr = c.get("data_refresh") or {}
    return {
        "app_name": c["app_name"], "warehouse_id": c["warehouse_id"],
        "lakebase_instance": c["lakebase"]["instance"], "lakebase_host": c["lakebase"]["host"],
        "lakebase_db": c["lakebase"]["database"], "secret_scope": c["mail"]["secret_scope"],
        "uc_catalog": c["uc"]["catalog"], "app_url": c["app"]["url"],
        "app_sp": c["lakebase"]["app_sp"],
        "test_mode": str(wr.get("test_mode", True)).lower(), "test_recipient": wr.get("test_recipient", ""),
        "days": str(wr.get("days", 7)), "schedule_cron": wr.get("schedule_cron", "0 0 9 ? * MON"),
        "timezone": wr.get("timezone", "UTC"),
        # Daily data-refresh job (rebuild ds_* from real Gateway tables → Lakebase).
        "refresh_cron": dr.get("schedule_cron", "0 0 6 * * ?"),
        "refresh_skip_classifier": str(dr.get("skip_classifier", False)).lower(),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--print-vars", action="store_true")
    args = ap.parse_args()
    c = yaml.safe_load(open(args.config))
    if args.print_vars:
        print(" ".join(f"--var {k}={json.dumps(v)}" for k, v in bundle_vars(c).items()))
    else:
        render_app_yaml(c)
        print("wrote app/app.yaml from", args.config)


if __name__ == "__main__":
    main()
