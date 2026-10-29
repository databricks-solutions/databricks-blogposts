#!/bin/bash
# usage: docs/export_flow_svg.sh  -> regenerates docs/{browser,replay}_flow.svg from docs/measurement_flow.html (needs Google Chrome)
cd "$(dirname "$0")"; set -- measurement_flow.html .
C="${CHROME:-/Applications/Google Chrome.app/Contents/MacOS/Google Chrome}"  # set CHROME on Linux/Windows
U="file://$(cd "$(dirname "$1")" && pwd | sed 's/ /%20/g')/$(basename "$1")"
for x in browser replay; do
  "$C" --headless=new --disable-gpu --user-data-dir=/tmp/chrome-shot --virtual-time-budget=3000 --dump-dom "$U#export-$x" 2>/dev/null |
    python3 -c "import sys,re; sys.stdout.write(re.search(r'<script type=\"text/plain\" id=\"svg-out\">(.*?)</script>', sys.stdin.read(), re.S).group(1))" > "$2/${x}_flow.svg"
  xmllint --noout "$2/${x}_flow.svg" && echo "$2/${x}_flow.svg"
done
