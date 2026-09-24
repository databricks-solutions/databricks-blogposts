import os

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="Embedded Dashboard Test")

WORKSPACE_HOST = os.environ.get("WORKSPACE_HOST", "").rstrip("/")
DASHBOARD_ID = os.environ.get("DASHBOARD_ID", "")

# AI/BI (Lakeview) published-dashboard embed URL.
DEFAULT_EMBED_URL = f"{WORKSPACE_HOST}/embed/dashboardsv3/{DASHBOARD_ID}" if WORKSPACE_HOST and DASHBOARD_ID else ""


@app.get("/healthz")
def healthz():
    return {"status": "ok", "embed_url": DEFAULT_EMBED_URL}


@app.get("/", response_class=HTMLResponse)
def index():
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Embedded Dashboard Test</title>
  <style>
    :root {{ color-scheme: light dark; }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
            background: #f6f7f9; color: #1b1f24; }}
    header {{ background: #1b3139; color: #fff; padding: 16px 20px; }}
    header h1 {{ margin: 0; font-size: 18px; font-weight: 600; }}
    header p {{ margin: 4px 0 0; font-size: 13px; opacity: 0.8; }}
    .bar {{ display: flex; gap: 8px; padding: 12px 20px; background: #fff; border-bottom: 1px solid #e3e6ea; }}
    .bar input {{ flex: 1; padding: 8px 10px; border: 1px solid #c6ccd4; border-radius: 6px; font-size: 13px; }}
    .bar button {{ padding: 8px 16px; border: 0; border-radius: 6px; background: #ff3621; color: #fff;
                   font-size: 13px; font-weight: 600; cursor: pointer; }}
    .wrap {{ padding: 16px 20px; }}
    .frame {{ width: 100%; height: calc(100vh - 190px); min-height: 480px; border: 1px solid #e3e6ea;
              border-radius: 8px; background: #fff; }}
    .note {{ padding: 0 20px 16px; font-size: 12px; color: #5a6470; }}
  </style>
</head>
<body>
  <header>
    <h1>External Embedded Dashboard Test</h1>
    <p>Databricks App hosting an AI/BI dashboard iframe to verify external embedding.</p>
  </header>
  <div class="bar">
    <input id="url" value="{DEFAULT_EMBED_URL}" placeholder="Paste a Databricks embed URL" />
    <button onclick="load()">Load</button>
  </div>
  <div class="wrap">
    <iframe id="frame" class="frame" src="{DEFAULT_EMBED_URL}"></iframe>
  </div>
  <div class="note">
    If the frame is blank, check the browser console for X-Frame-Options / CSP errors, and confirm this app's
    domain is on the workspace's AI/BI embedding approved-domains list.
  </div>
  <script>
    function load() {{
      var u = document.getElementById('url').value.trim();
      if (u) document.getElementById('frame').src = u;
    }}
  </script>
</body>
</html>"""
