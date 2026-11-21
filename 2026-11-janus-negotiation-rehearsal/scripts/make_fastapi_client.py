"""Generate TypeScript client from FastAPI OpenAPI spec."""

import json
import sys
from pathlib import Path

import httpx


def main():
    base_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000"
    openapi_url = f"{base_url}/openapi.json"

    print(f"Fetching OpenAPI spec from {openapi_url}...")
    resp = httpx.get(openapi_url, timeout=10)
    resp.raise_for_status()
    spec = resp.json()

    client_dir = Path(__file__).parent.parent / "client" / "src" / "fastapi_client"
    client_dir.mkdir(parents=True, exist_ok=True)

    # Write spec
    (client_dir / "openapi.json").write_text(json.dumps(spec, indent=2))

    # Generate a simple typed client
    paths = spec.get("paths", {})
    lines = [
        '// Auto-generated from OpenAPI spec — do not edit',
        '',
        'const BASE = "";  // proxy handles routing',
        '',
        'async function request<T>(method: string, path: string, body?: unknown): Promise<T> {',
        '  const opts: RequestInit = {',
        '    method,',
        '    headers: { "Content-Type": "application/json" },',
        '  };',
        '  if (body !== undefined) opts.body = JSON.stringify(body);',
        '  const res = await fetch(`${BASE}${path}`, opts);',
        '  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);',
        '  return res.json();',
        '}',
        '',
        'export const apiClient = {',
    ]

    for path, methods in sorted(paths.items()):
        for method, op in methods.items():
            op_id = op.get("operationId", "")
            if not op_id:
                continue
            func_name = op_id.replace("-", "_")
            if method.upper() == "GET":
                lines.append(f'  {func_name}: (params?: Record<string, string>) => {{')
                lines.append(f'    const q = params ? "?" + new URLSearchParams(params).toString() : "";')
                lines.append(f'    return request<any>("GET", `{path}${{q}}`);')
                lines.append('  },')
            elif method.upper() in ("POST", "PUT", "PATCH"):
                lines.append(f'  {func_name}: (body?: any) => request<any>("{method.upper()}", "{path}", body),')
            elif method.upper() == "DELETE":
                lines.append(f'  {func_name}: () => request<any>("DELETE", "{path}"),')

    lines.append("};")
    lines.append("")

    (client_dir / "index.ts").write_text("\n".join(lines))
    print(f"Client generated at {client_dir / 'index.ts'}")


if __name__ == "__main__":
    main()
