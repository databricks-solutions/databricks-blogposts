"""Render blog/blog_post.md (with both SVG charts inlined) to blog/blog_post.pdf via headless Chrome.

Usage: python chart.py && python blog_pdf.py
"""
import os
import re
import subprocess

import markdown

from chart import CHROME, HERE

CSS = """
@page { size: A4; margin: 15mm 16mm; }
body { font: 10.5pt/1.5 system-ui, -apple-system, "Segoe UI", sans-serif; color: #0b0b0b; max-width: 100%; }
h1 { font-size: 20pt; line-height: 1.2; margin: 0 0 4pt; }
h2 { font-size: 13.5pt; margin: 18pt 0 6pt; break-after: avoid; }
p { margin: 0 0 7pt; } li { margin: 0 0 4pt; } ul, ol { padding-left: 18pt; }
.fig { margin: 6pt auto 2pt; width: 92%; break-inside: avoid; } .fig svg { width: 100%; height: auto; }
.fig + p { break-before: avoid; }
em { color: #52514e; }
code { font: 9pt ui-monospace, Menlo, monospace; background: #f1f0ec; padding: 0 3pt; border-radius: 3pt; }
table { border-collapse: collapse; width: 100%; font-size: 8.5pt; font-variant-numeric: tabular-nums; margin: 6pt 0 10pt;
  break-inside: avoid; }
th, td { padding: 4pt 5pt; border-bottom: 1px solid #e1e0d9; vertical-align: top; }
th { text-align: left; border-bottom: 1.5px solid #c3c2b7; }
td:first-child { width: 28%; }
td:not(:first-child), th:not(:first-child) { text-align: right; white-space: nowrap; }
img { display: block; max-width: 70%; margin: 6pt auto 2pt; border: 1px solid #e1e0d9; border-radius: 6pt; }
"""


def main():
    blog = os.path.join(HERE, "blog")
    md = open(os.path.join(blog, "blog_post.md")).read()
    md = re.sub(r"^((?:  )+)(?=- )", lambda m: m.group(1) * 2, md, flags=re.M)
    body = markdown.markdown(md, extensions=["tables"])
    # a PDF cannot animate: use the static screenshot instead of the scrolling GIF
    body = body.replace('src="dashboard.gif"', 'src="dashboard.png"')
    body = re.sub(r'<img alt="[^"]*" src="([\w_]+\.svg)" ?/?>',
                  lambda m: f'<div class="fig">{open(os.path.join(blog, m.group(1))).read()}</div>', body)
    html_path = os.path.join(blog, "blog_post_print.html")
    open(html_path, "w").write(f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Blog post</title>'
                               f'<style>{CSS}</style></head><body>{body}</body></html>')
    out = os.path.join(blog, "blog_post.pdf")
    subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--no-pdf-header-footer", f"--print-to-pdf={out}",
                    "file://" + html_path], capture_output=True, check=True)
    os.remove(html_path)
    print(out)


if __name__ == "__main__":
    main()
