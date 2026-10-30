"""Interactive UX helpers — pacing, narration, SQL display, spinners, workspace links.

Designed for impactful in-front-of-an-audience demos: every slow operation shows
an animated spinner (never a frozen screen), the exact SQL is shown before it runs,
a numbered agenda + per-step progress bar shows what's done and what's next, and the
presenter advances at their own pace with Enter.

Set DEMO_AUTO=1 to run non-interactively (no Enter prompts, no spinner) — used for
headless validation / rehearsal.
"""
from __future__ import annotations

import atexit
import itertools
import os
import re
import shutil
import sys
import textwrap
import threading
import time
import unicodedata

AUTO = os.environ.get("DEMO_AUTO", "0") == "1"

# --- cursor visibility ------------------------------------------------------
# Hide the terminal cursor for the whole demo (so it doesn't flicker during the
# scroll animation, spinners, or static output); show it only at the Enter
# prompt; always restore it on exit.
_TTY = sys.stdout.isatty()


def _hide_cursor() -> None:
    if not AUTO and _TTY:
        sys.stdout.write("\033[?25l"); sys.stdout.flush()


def _show_cursor() -> None:
    if not AUTO and _TTY:
        sys.stdout.write("\033[?25h"); sys.stdout.flush()


_hide_cursor()
atexit.register(_show_cursor)


def _flush_input() -> None:
    """Discard any keystrokes typed during the previous (often slow) step.
    Without this, an Enter pressed impatiently while Spark/CTAS is running gets
    buffered and instantly satisfies the next `Press Enter` prompt — so the step
    appears not to wait and its result flashes past unread. Draining stdin makes
    every advance a fresh, deliberate keypress made after the output is visible."""
    if AUTO or not _TTY:
        return
    try:
        import termios
        termios.tcflush(sys.stdin, termios.TCIFLUSH)
    except Exception:  # noqa: BLE001  (non-Unix / no tty — nothing to flush)
        pass

# ANSI styles
_R = "\033[0m"; _B = "\033[1m"; _D = "\033[2m"
CY = "\033[36m"; GR = "\033[32m"; YE = "\033[33m"; BL = "\033[34m"; GY = "\033[90m"; MA = "\033[35m"
WH = "\033[97m"          # bright white
NARR = "\033[38;5;79m"   # soft aquamarine-green — readable narration that "pops"


def _wrap(msg: str) -> list[str]:
    width = min(shutil.get_terminal_size((100, 24)).columns - 6, 108)
    return textwrap.fill(msg, width=max(40, width)).splitlines()


# Move to the next step with a smooth upward SCROLL (no clearing): emit the
# newlines one at a time so the previous step visibly slides up into scrollback
# (preserved — scroll up to review), then park the cursor a couple of lines below
# the top so the new step lands near the TOP of the screen (a small breathing
# margin, not glued to row 1, and not a big gap). The animation makes the
# transition easy to follow. On by default; DEMO_PAGE=0 disables;
# DEMO_SCROLL_SECS tunes the duration; DEMO_TOP_LINES = blank lines above the step.
PAGE = os.environ.get("DEMO_PAGE", "1") != "0"
SCROLL_SECS = float(os.environ.get("DEMO_SCROLL_SECS", "1.2"))
TOP_LINES = int(os.environ.get("DEMO_TOP_LINES", "2"))


def page() -> None:
    if AUTO or not PAGE:
        return
    h = shutil.get_terminal_size((100, 40)).lines
    delay = SCROLL_SECS / max(h, 1)   # uncapped → one smooth, even glide
    for _ in range(h):
        sys.stdout.write("\n")
        sys.stdout.flush()
        time.sleep(delay)
    top = max(1, TOP_LINES + 1)   # new step starts near the top, small margin above
    sys.stdout.write(f"\033[{top};1H")
    sys.stdout.flush()


# After the scroll, reveal the new step's lines one at a time (a gentle pause per
# line) so the content eases up onto the screen instead of popping in at once.
# DEMO_REVEAL_SECS tunes the per-line pause (0 disables).
REVEAL_SECS = float(os.environ.get("DEMO_REVEAL_SECS", "0.05"))


def _emit(line: str = "") -> None:
    print(line)
    if not AUTO and _TTY and PAGE and REVEAL_SECS > 0:
        time.sleep(REVEAL_SECS)


class Agenda:
    """Numbered agenda + per-step progress bar."""

    def __init__(self, title: str, steps: list[str]):
        self.title = title
        self.steps = steps
        self.i = -1

    def render(self) -> None:
        bar = "=" * (len(self.title) + 4)
        _emit(f"\n{_B}{MA}{bar}{_R}")
        _emit(f"{_B}{MA}  {self.title}{_R}")
        _emit(f"{_B}{MA}{bar}{_R}")
        for n, s in enumerate(self.steps, 1):
            _emit(f"   {GY}○ {n}. {s}{_R}")

    def tick(self) -> None:
        """Advance to the next step; print a compact progress line with a
        number bar — done = green, current = highlighted, remaining = light blue.
        From step 2 on, scroll the previous step up (kept in scrollback) so this
        one renders near the top of the screen."""
        self.i += 1
        if self.i >= 1:
            page()
        cur = "\033[1;93m"   # bold bright yellow (highlighted current)
        rem = "\033[94m"     # light blue (remaining)
        last = len(self.steps) - 1
        nums = []
        for k in range(len(self.steps)):
            n = k + 1
            if k < self.i:
                nums.append(f"{GR}{n}{_R}")                 # done → green
            elif k == self.i:
                # the final step has no "next" — show it green (complete), not highlighted
                nums.append(f"{GR}{n}{_R}" if self.i == last else f"{cur}{n}{_R}")
            else:
                nums.append(f"{rem}{n}{_R}")                # remaining → light blue
        nxt = self.steps[self.i + 1] if self.i + 1 < len(self.steps) else "done 🎉"
        _emit(f"\n[{' '.join(nums)}]   {_B}Step {self.i + 1}/{len(self.steps)}{_R} · "
              f"{CY}{_B}{self.steps[self.i]}{_R}   {GY}→ next: {nxt}{_R}\n")

    def checkpoint(self, title: str) -> None:
        """Mid-demo recap: steps completed so far green-ticked, the rest as ○ (remaining)."""
        bar = "=" * (len(title) + 4)
        _emit(f"\n{_B}{MA}{bar}{_R}")
        _emit(f"{_B}{MA}  {title}{_R}")
        _emit(f"{_B}{MA}{bar}{_R}")
        for n, s in enumerate(self.steps, 1):
            if n - 1 <= self.i:
                _emit(f"   {GR}✓{_R} {GY}{n}. {s}{_R}")
            else:
                _emit(f"   {GY}○ {n}. {s}{_R}")
        _emit()

    def recap(self, title: str = "Done — all steps complete 🎉", failed: set[int] = frozenset()) -> None:
        """Final recap: the agenda in its original style, every bullet green-ticked
        except the `failed` steps (0-based), which get a ✗."""
        bar = "=" * (len(title) + 4)
        _emit(f"\n{_B}{MA}{bar}{_R}")
        _emit(f"{_B}{MA}  {title}{_R}")
        _emit(f"{_B}{MA}{bar}{_R}")
        for n, s in enumerate(self.steps, 1):
            mark = f"{YE}✗{_R}" if n - 1 in failed else f"{GR}✓{_R}"
            _emit(f"   {mark} {GY}{n}. {s}{_R}")
        _emit()


STOPPED = 130  # exit status when the presenter stops at a prompt (Ctrl-C / EOF)


def step(prompt: str = "continue") -> None:
    """Wait for the presenter to press Enter (unless DEMO_AUTO).
    Blank line before; trailing ` ...` where only the dots blink.
    Stopping at the prompt exits with STOPPED."""
    if AUTO:
        print(f"{_D}   (auto) {prompt}{_R}")
        return
    _flush_input()   # drop keystrokes buffered during the (slow) step just shown
    _show_cursor()
    try:
        input(f"   {CY}{_B}▶ Press Enter to {prompt}{_R} \033[5m...\033[0m ")
    except (EOFError, KeyboardInterrupt):
        print(f"\n   {YE}⏹ stopped.{_R}")
        raise SystemExit(STOPPED)
    finally:
        _hide_cursor()


def narrate(msg: str) -> None:
    """Readable narration (soft green — pops, signals 'done'), wrapped to a paragraph."""
    for ln in _wrap(msg):
        _emit(f"   {NARR}{ln}{_R}")
    _emit()


def info(msg: str) -> None:
    for ln in _wrap(msg):
        _emit(f"   {GY}{ln}{_R}")
    _emit()


def blocked(msg: str) -> None:
    """A permission-denied / access-blocked message, shown as a distinct amber 🔒
    block. In these governance demos the block is the EXPECTED, desired outcome,
    so it reads as a deliberate enforcement signal — not a red crash."""
    amber = "\033[38;5;208m"
    lines = _wrap(msg)
    _emit(f"   {amber}{_B}🔒 {lines[0] if lines else ''}{_R}")
    for ln in lines[1:]:
        _emit(f"      {amber}{_B}{ln}{_R}")
    _emit()


# --- SQL syntax highlighting (token-based; safe inside a colored background) --
_FG = "\033[39m"      # reset foreground only (keeps background)
_KW = {
    "CREATE", "OR", "REPLACE", "TABLE", "DROP", "IF", "EXISTS", "USING",
    "TBLPROPERTIES", "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "ASC",
    "DESC", "INSERT", "INTO", "VALUES", "AS", "DESCRIBE", "EXTENDED", "SHOW",
    "TABLES", "SCHEMAS", "SCHEMA", "CATALOG", "IN", "AND", "NOT", "ON", "SET",
    "ALTER", "COLUMN", "POLICY", "MASK", "FILTER", "ROW", "MATCH", "COLUMNS",
    "TO", "FOR", "WHEN", "EXCEPT", "RETURN", "RETURNS", "FUNCTION", "CASE",
    "THEN", "ELSE", "END", "LIMIT", "GRANT", "USE", "COMMENT", "TAGS",
    "EXTERNAL", "CASCADE", "COUNT", "SUM", "AVG", "MIN", "MAX",
    "VIEW", "JOIN", "REVOKE", "DISTINCT", "BETWEEN", "ICEBERG", "MANAGED", "LOCATION",
}
_TY = {
    "BIGINT", "STRING", "DECIMAL", "INT", "INTEGER", "BOOLEAN", "DATE",
    "TIMESTAMP", "DOUBLE", "FLOAT", "DELTA",
}


def _tok(t: str) -> str:
    up = t.upper()
    if up in _KW:
        return f"\033[1m\033[35m{t}\033[22m{_FG}"   # bold magenta keyword
    if up in _TY:
        return f"{CY}{t}{_FG}"                       # cyan type
    if re.fullmatch(r"\d+(?:\.\d+)?", t):
        return f"{YE}{t}{_FG}"                       # yellow number
    return t


def _highlight(line: str) -> str:
    out = []
    for seg in re.split(r"('[^']*')", line):
        if len(seg) >= 2 and seg[0] == "'" and seg[-1] == "'":
            out.append(f"{GR}{seg}{_FG}")            # green string literal
        else:
            out.append("".join(_tok(t) for t in re.split(r"(\W+)", seg)))
    return "".join(out)


def show_sql(sql: str) -> None:
    """Render SQL as a syntax-highlighted, shaded code block (distinct from prose)."""
    lines = sql.strip("\n").splitlines()
    width = max((len(l) for l in lines), default=0)
    bg = "\033[48;5;236m"   # dark grey background → looks like an IDE code block
    _emit(f"   {_D}── SQL ─{'─' * max(2, width - 4)}{_R}")
    for l in lines:
        pad = " " * (width - len(l) + 2)
        _emit(f"   {bg}  {_highlight(l)}{pad}{_R}")
    _emit()


def link(label: str, url: str) -> None:
    _emit(f"   {GR}🔗 {label}:{_R}\n      {BL}{_B}{url}{_R}\n")


def _dwidth(s: str) -> int:
    """Display width of a string in terminal columns — counts wide CJK glyphs and
    emoji/symbols (e.g. ❌ U+274C) as 2, combining marks as 0. `len()` undercounts
    these, which breaks table alignment."""
    w = 0
    for ch in str(s):
        if unicodedata.combining(ch):
            continue
        o = ord(ch)
        if unicodedata.east_asian_width(ch) in ("W", "F") \
           or 0x2600 <= o <= 0x27BF or 0x2B00 <= o <= 0x2BFF or 0x1F000 <= o <= 0x1FAFF:
            w += 2
        else:
            w += 1
    return w


def _ctr(s: str, w: int) -> str:
    pad = w - _dwidth(s)
    left = pad // 2
    return " " * left + s + " " * (pad - left)


def show_table(headers: list[str], rows: list[list]) -> None:
    """Render a clean, center-aligned table (Spark's .show() can't center).
    Widths use display width so wide glyphs (e.g. masked ❌❌❌❌❌) stay aligned."""
    cols = [str(h) for h in headers]
    data = [["" if v is None else str(v) for v in r] for r in rows]
    widths = [max(_dwidth(cols[i]), *(_dwidth(r[i]) for r in data)) if data else _dwidth(cols[i])
              for i in range(len(cols))]
    border = "   {}".format("+" + "+".join("-" * (w + 2) for w in widths) + "+")
    _emit(border)
    _emit("   |" + "|".join(f" {CY}{_B}{_ctr(cols[i], widths[i])}{_R} " for i in range(len(cols))) + "|")
    _emit(border)
    for r in data:
        _emit("   |" + "|".join(f" {_ctr(r[i], widths[i])} " for i in range(len(cols))) + "|")
    _emit(border)
    _emit()


def show_df(df) -> None:
    """Collect a Spark DataFrame and render it center-aligned."""
    show_table(df.columns, [list(row) for row in df.collect()])


def show_config(title: str, pairs: list[tuple[str, str]], highlight: set[str] | None = None) -> None:
    """Render a key→value config panel as a shaded IDE-style block (same look as
    show_sql). Used to surface the LIVE engine configuration at the top of a demo —
    proof that the operations are authentic. Labels listed in `highlight` are
    emphasized (★ + bright yellow), e.g. server-side planning in Demo 3. Values may
    contain newlines (rendered as continuation rows under a blank label)."""
    highlight = highlight or set()
    bg = "\033[48;5;236m"            # dark grey background (matches show_sql)
    HL = "\033[1m\033[93m"           # bold bright yellow foreground (keeps bg)
    label_w = max((_dwidth(k) for k, _ in pairs), default=0)
    lines: list[tuple[str, bool]] = []
    for k, v in pairs:
        hl = k in highlight
        for j, vl in enumerate(str(v).split("\n") or [""]):
            lbl = k if j == 0 else ""
            prefix = "★ " if hl else "  "
            lpad = " " * (label_w - _dwidth(lbl))
            lines.append((f"{prefix}{lbl}{lpad}   {vl}", hl))
    width = max((_dwidth(t) for t, _ in lines), default=0)
    bar = "─" * max(2, width - _dwidth(title) + 1)
    _emit(f"   {_D}── {title} {bar}{_R}")
    for text, hl in lines:
        pad = " " * (width - _dwidth(text) + 2)
        body = f"{HL}{text}{_FG}" if hl else text
        _emit(f"   {bg}  {body}{pad}{_R}")
    _emit()


class working:
    """Context manager: animated spinner during a slow operation."""

    def __init__(self, msg: str):
        self.msg = msg
        self._stop = threading.Event()
        self.t = None
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.time()
        if AUTO:
            print(f"   {self.msg} …")
            return self

        def spin():
            for ch in itertools.cycle("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"):
                if self._stop.is_set():
                    break
                sys.stdout.write(f"\r   {CY}{ch}{_R} {self.msg} ")
                sys.stdout.flush()
                time.sleep(0.1)

        self.t = threading.Thread(target=spin, daemon=True)
        self.t.start()
        return self

    def __exit__(self, exc_type, *exc):
        self._stop.set()
        if self.t:
            self.t.join()
        mark = "✗" if exc_type else "✓"   # ✗ when the block raised
        if not AUTO:
            sys.stdout.write(f"\r   {YE if exc_type else GR}{mark}{_R} {self.msg}\033[K\n\n")
            sys.stdout.flush()
        else:
            print(f"   {mark} {self.msg}\n")


def schema_url(host: str, catalog: str, schema: str) -> str:
    return f"{host}/explore/data/{catalog}/{schema}"
