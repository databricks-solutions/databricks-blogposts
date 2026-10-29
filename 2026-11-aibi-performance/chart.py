"""Bar charts of the browser first load: isolated (each measure alone) and stacked (cumulative), % always vs the baseline.

Usage: python chart.py  -> writes blog/{isolated,stacked}_chart.html (interactive) and .svg (static, for the blog post)
"""
import html
import json
import os
import re
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = ("l0_baseline", "Baseline", "LC + metric view")
# isolated: baseline first, then from the smallest to the largest single gain
ISOLATED = [BASE, ("li3_fewwidgets", "+ Lean", "landing page"), ("li2_matgrain", "+ Materialized", "aggregate"),
            ("li4_ignfilters", "+ Dropdowns from", "small tables"), ("li1_prejoined", "+ Pre-joined", "wide table"),
            ("li5_lakehousert", "+ Lakehouse//RT", "warehouse")]
STACKED = [BASE, ("li1_prejoined", "+ Pre-joined", "wide table"), ("ls2_matgrain", "+ Materialized", "aggregate"),
           ("ls3_fewwidgets", "+ Lean", "landing page"), ("ls4_ignfilters", "+ Dropdowns from", "small tables"),
           ("ls5_lakehousert", "+ Lakehouse//RT", "warehouse")]


def runs(k):
    return json.load(open(os.path.join(HERE, "results", f"browser_{k}.json")))["first_load_runs_s"]


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>AI/BI dashboard benchmark - first load per step</title>
<style>
.viz-root {
  color-scheme: light;
  --page: #f9f9f7; --surface-1: #fcfcfb; --text-primary: #0b0b0b; --text-secondary: #52514e; --muted: #898781;
  --grid: #e1e0d9; --axis: #c3c2b7; --border: rgba(11,11,11,0.10);
  --series-1: #2a78d6;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) .viz-root {
    color-scheme: dark;
    --page: #0d0d0d; --surface-1: #1a1a19; --text-primary: #f0efec; --text-secondary: #c3c2b7; --muted: #898781;
    --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
    --series-1: #3987e5;
  }
}
:root[data-theme="dark"] .viz-root {
  color-scheme: dark;
  --page: #0d0d0d; --surface-1: #1a1a19; --text-primary: #f0efec; --text-secondary: #c3c2b7; --muted: #898781;
  --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
  --series-1: #3987e5;
}
body { margin: 0; }
.viz-root { background: var(--page); color: var(--text-primary); font: 13px/1.4 system-ui, -apple-system, "Segoe UI", sans-serif;
  padding: 24px; min-height: 100vh; box-sizing: border-box; }
.card { background: var(--surface-1); border: 1px solid var(--border); border-radius: 12px; padding: 20px 24px; max-width: 820px; }
h1 { font-size: 17px; font-weight: 600; margin: 0 0 4px; }
svg text { font-family: inherit; }
.gl { stroke: var(--grid); stroke-width: 1; } .base { stroke: var(--axis); stroke-width: 1; }
.ax { fill: var(--muted); font-size: 11px; font-variant-numeric: tabular-nums; }
.lab { fill: var(--text-primary); font-size: 12px; } .lab2 { fill: var(--text-secondary); font-size: 11px; }
.val { fill: var(--text-primary); font-size: 11px; font-weight: 600; font-variant-numeric: tabular-nums; }
.chg { fill: var(--text-secondary); font-size: 10px; font-variant-numeric: tabular-nums; }
.bar { fill: var(--series-1); } .bar.on { fill-opacity: .82; }
.err { stroke: var(--text-primary); stroke-width: 2; stroke-linecap: round; }
.hit { fill: transparent; }
#tip { position: fixed; pointer-events: none; background: var(--surface-1); color: var(--text-primary); border: 1px solid var(--border);
  border-radius: 8px; padding: 6px 10px; box-shadow: 0 4px 14px rgba(0,0,0,.12); font-size: 12px; display: none; white-space: nowrap; }
#tip b { font-weight: 600; } #tip .m { color: var(--text-secondary); }
</style></head>
<body><div class="viz-root"><div class="card">
<h1>__TITLE__</h1>
  <div class="panel"><svg id="p-steps"></svg></div>
</div><div id="tip"></div></div>
<script>
const DATA = __DATA__;
const H = 290, padTop = 28, bottom = 46, axisW = 34, gap = 18;
const mean = a => a.reduce((s, v) => s + v, 0) / a.length;
const sd = a => { const m = mean(a); return Math.sqrt(a.reduce((s, v) => s + (v - m) ** 2, 0) / (a.length - 1)); };
const tip = document.getElementById('tip');
const NS = 'http://www.w3.org/2000/svg';
const el = (tag, attrs, parent, text) => { const e = document.createElementNS(NS, tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]); if (text != null) e.textContent = text; parent.appendChild(e); return e; };
// bar with 4px rounded top, square at the baseline
const barPath = (x, y, w, h) => { const r = Math.min(4, h, w / 2);
  return `M${x},${y + h}V${y + r}Q${x},${y} ${x + r},${y}H${x + w - r}Q${x + w},${y} ${x + w},${y + r}V${y + h}Z`; };

function panel(id, rows, W, YMAX, title) {
  const svg = document.getElementById(id), plotH = H - padTop - bottom;
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`); svg.setAttribute('width', '100%');
  svg.setAttribute('role', 'img'); svg.setAttribute('aria-label', title);
  const y = v => padTop + plotH - (v / YMAX) * plotH;
  for (let t = 0; t <= YMAX; t += 2) {
    el('line', {class: t ? 'gl' : 'base', x1: axisW, x2: W, y1: y(t), y2: y(t)}, svg);
    el('text', {class: 'ax', x: axisW - 6, y: y(t) + 4, 'text-anchor': 'end'}, svg, t + (t === YMAX ? ' s' : ''));
  }
  const slot = (W - axisW) / rows.length, bw = Math.min(64, slot - gap);
  rows.forEach((d, i) => {
    const m = mean(d.runs), s = sd(d.runs), cx = axisW + slot * (i + 0.5), x0 = cx - bw / 2;
    const bar = el('path', {class: 'bar', d: barPath(x0, y(m), bw, y(0) - y(m))}, svg);
    el('line', {class: 'err', x1: cx, x2: cx, y1: y(m + s), y2: y(Math.max(0, m - s))}, svg);
    [m + s, Math.max(0, m - s)].forEach(v => el('line', {class: 'err', x1: cx - 6, x2: cx + 6, y1: y(v), y2: y(v)}, svg));
    const top = y(m + s);
    el('text', {class: 'val', x: cx, y: top - (i ? 18 : 6), 'text-anchor': 'middle'}, svg, `${m.toFixed(2)} ± ${s.toFixed(2)} s`);
    if (i) { const p = mean(rows[0].runs);
      el('text', {class: 'chg', x: cx, y: top - 6, 'text-anchor': 'middle'}, svg, (c => `${c > 0 ? '+' : c < 0 ? '−' : '±'}${Math.abs(c)}%`)(Math.round((m / p - 1) * 100))); }
    el('text', {class: 'lab', x: cx, y: H - bottom + 18, 'text-anchor': 'middle'}, svg, d.l1);
    el('text', {class: 'lab2', x: cx, y: H - bottom + 33, 'text-anchor': 'middle'}, svg, d.l2);
    const hit = el('rect', {class: 'hit', x: cx - slot / 2, y: padTop - 20, width: slot, height: plotH + 20}, svg);
    hit.addEventListener('mousemove', ev => {
      bar.classList.add('on'); tip.style.display = 'block';
      tip.innerHTML = `<b>${d.l1} ${d.l2}</b><br>mean <b>${m.toFixed(2)} s</b> ± ${s.toFixed(2)} s<br><span class="m">10 runs, ${Math.min(...d.runs).toFixed(2)}–${Math.max(...d.runs).toFixed(2)} s</span>`;
      tip.style.left = (ev.clientX + 14) + 'px'; tip.style.top = (ev.clientY + 14) + 'px';
    });
    hit.addEventListener('mouseleave', () => { bar.classList.remove('on'); tip.style.display = 'none'; });
  });
}
panel('p-steps', DATA.steps, 720, 12, '__ARIA__');

</script></body></html>
"""


CHROME = os.environ.get("CHROME_PATH", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
# light theme, resolved so the SVG renders without CSS variables (GitHub, Confluence, Inkscape, ...)
SVG_STYLE = """text { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; font-variant-numeric: tabular-nums; }
.h1 { fill: #0b0b0b; font-size: 17px; font-weight: 600; }
.gl { stroke: #e1e0d9; stroke-width: 1; } .base { stroke: #c3c2b7; stroke-width: 1; }
.ax { fill: #898781; font-size: 11px; } .lab { fill: #0b0b0b; font-size: 12px; } .lab2 { fill: #52514e; font-size: 11px; }
.val { fill: #0b0b0b; font-size: 11px; font-weight: 600; } .chg { fill: #52514e; font-size: 10px; }
.bar { fill: #2a78d6; } .err { stroke: #0b0b0b; stroke-width: 2; stroke-linecap: round; } .hit { display: none; }"""


def write_svg(html_path, out):
    """Let Chrome draw the chart, then wrap the drawn <svg> with its title into a standalone SVG file."""
    dom = subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--dump-dom", "file://" + html_path],
                         capture_output=True, text=True, check=True).stdout
    plot = re.search(r'<svg id="p-steps"[^>]*viewBox="0 0 (\d+) (\d+)"[^>]*>(.*?)</svg>', dom, re.S)
    pw, ph, body = int(plot.group(1)), int(plot.group(2)), plot.group(3)
    title = re.search(r"<h1>(.*?)</h1>", dom).group(1)
    W, pad, top = 840, 24, 48  # title only; the blog caption explains the chart
    sh = round(ph * (W - 2 * pad) / pw)  # plot scaled to the card width
    open(out, "w").write(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{top + sh + 8}" viewBox="0 0 {W} {top + sh + 8}" '
        f'role="img" aria-label="{html.escape(html.unescape(title))}"><style>{SVG_STYLE}</style>'
        f'<rect width="100%" height="100%" rx="12" fill="#fcfcfb" stroke="rgba(11,11,11,0.10)"/>'
        f'<text class="h1" x="{pad}" y="34">{title}</text>'
        f'<svg x="{pad}" y="{top}" width="{W - 2 * pad}" height="{sh}" viewBox="0 0 {pw} {ph}">{body}</svg></svg>\n')
    print(out)


def write(name, bars, title, aria):
    steps = [{"l1": l1, "l2": l2, "runs": runs(k)} for k, l1, l2 in bars]
    out = os.path.join(HERE, "blog", f"{name}_chart.html")
    open(out, "w").write(PAGE.replace("__TITLE__", title).replace("__ARIA__", aria)
                         .replace("__DATA__", json.dumps({"steps": steps})))
    print(out)
    write_svg(out, os.path.join(HERE, "blog", f"{name}_chart.svg"))


def main():
    os.makedirs(os.path.join(HERE, "blog"), exist_ok=True)
    first, last = (sum(runs(k)) / len(runs(k)) for k in (STACKED[0][0], STACKED[-1][0]))
    write("isolated", ISOLATED, "Cold landing-page load, each measure on its own",
          "Browser first load of the baseline and of each measure applied alone to the baseline")
    write("stacked", STACKED, "Cold landing-page load per optimization step",
          f"Browser first load per cumulative step, descending from {first:.1f} s to {last:.1f} s")


if __name__ == "__main__":
    main()
