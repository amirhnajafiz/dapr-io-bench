#!/usr/bin/env python3
"""Charts benchmark traces (traces/*.jsonl) as SVG line charts and a summary table.

    scripts/plot_results.py traces/*.jsonl [-o plots/]

One SVG per backend, operation and load dimension, e.g.
plots/redis/write_vs_agents.svg: four panels (p50 latency, p99 latency,
throughput, error rate) with the dimension on the x axis (log scale), one
line for the direct path and one for Dapr. Next to the charts,
plots/<backend>/summary.md tabulates every point with the Dapr-vs-direct
ratio, so a number can be quoted without reading a chart.

Pure standard library on purpose: the output is SVG and Markdown, so the
project stays runnable with nothing but python3.
"""

import argparse
import json
import math
import os
import sys
from collections import OrderedDict, defaultdict

# Two categorical slots, fixed order: direct is always blue, dapr always orange,
# in every chart. Colours follow the entity, never its rank or value, so a
# reader learns the mapping once. Verified colourblind-safe (worst-pair
# deltaE 24.7 under protanopia, well clear of the 8.0 floor).
RGB = {"direct": "#2a78d6", "dapr": "#eb6834"}
MODES = ("direct", "dapr")
SURFACE = "#fcfcfb"  # chart surface, painted explicitly
INK = "#0b0b0b"  # primary text
INK_MUTED = "#52514e"  # secondary text: values, axis labels
GRID = "#dfdeda"  # recessive hairline gridlines

DIMENSIONS = OrderedDict(
    [
        ("agents", {"label": "concurrent agents", "slug": "agents"}),
        ("payload_bytes", {"label": "payload size", "slug": "payload"}),
    ]
)

BACKEND_NAMES = {"nats": "NATS", "postgres": "PostgreSQL", "redis": "Redis", "fs": "Filesystem"}
OP_NAMES = {"stat": "stat (light, no payload)", "publish": "publish", "write": "write", "read": "read"}


def held_text(dimension, held):
    """Describes the dimension a chart holds fixed, e.g. "256 B payload"."""
    others = [d for d in DIMENSIONS if d != dimension]
    parts = []
    for d, v in zip(others, held):
        if d == "payload_bytes":
            parts.append(f"{fmt_x(v, d)} payload")
        else:
            parts.append(f"{v:,} agent" + ("s" if v != 1 else ""))
    return ", ".join(parts)

# Panels, in display order: (title, unit, scale, extractor, higher_is_better)
METRICS = OrderedDict(
    [
        ("latency_p50", ("p50 latency", "ms", 1e3, lambda r: r["latency"]["p50"], False)),
        ("latency_p99", ("p99 latency", "ms", 1e3, lambda r: r["latency"]["p99"], False)),
        ("throughput", ("throughput", "ops/s", 1.0, lambda r: r["throughput"], True)),
        ("errors", ("error rate", "%", 1.0, lambda r: 100.0 * r["errors"] / r["ops"] if r["ops"] else 0.0, False)),
    ]
)
# Tabulated but not charted: the time for all tasks of the step to finish,
# which is what a group of agents waits for.
TABLE_METRICS = dict(METRICS)
TABLE_METRICS["makespan"] = ("makespan", "s", 1.0, lambda r: r["elapsed_seconds"], False)


# --- Loading -----------------------------------------------------------------


def load(paths):
    """Reads trace files into a record list, the latest record winning when
    two files measured the same connector/op at the same point."""
    latest = OrderedDict()
    for path in paths:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if rec.get("type") == "step":
                    key = (
                        rec["backend"], rec["mode"], rec["op"], rec.get("dimension"),
                        rec["agents"], rec["payload_bytes"],
                    )
                    if key in latest and latest[key]["captured_at"] > rec["captured_at"]:
                        continue
                    latest[key] = rec
    return list(latest.values())


def series(records, dimension):
    """Slices records along one dimension.

    Returns {(op, held): {mode: [(x, record), ...]}} where held is the tuple
    of the other two dimensions' values. In one-at-a-time mode every dimension
    has exactly one held group (the baseline); a grid run has one group per
    combination of the other two dimensions.
    """
    others = [d for d in DIMENSIONS if d != dimension]
    out = defaultdict(lambda: defaultdict(list))
    for r in records:
        # only points measured for this dimension (plus the shared baseline and
        # grid points): otherwise an agents-sweep point from one run and a
        # payload-sweep point from another could pair up into a bogus curve
        tags = set(r.get("dimension", dimension).split(","))
        if not tags & {dimension, "baseline", "grid"}:
            continue
        held = tuple(r[d] for d in others)
        out[(r["op"], held)][r["mode"]].append((r[dimension], r))
    for group in out.values():
        for mode in group:
            group[mode].sort(key=lambda p: p[0])
    # a group needs at least two x values to be a curve
    return {
        k: g for k, g in out.items()
        if len({x for pts in g.values() for x, _ in pts}) >= 2
    }


# --- SVG helpers -------------------------------------------------------------


def esc(text):
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def nice_ticks(top, count=4):
    """Rounds the axis maximum up to a readable step, returning (max, ticks)."""
    if top <= 0:
        return 1.0, [0.0, 1.0]
    raw = top / count
    magnitude = 10 ** math.floor(math.log10(raw))
    for factor in (1, 2, 2.5, 5, 10):
        step = magnitude * factor
        if step * count >= top:
            break
    return step * count, [step * i for i in range(count + 1)]


def fmt(value, unit):
    """A data label: three significant figures, never rounding a small value
    to zero (an error rate of 0.033% must not print as 0.0)."""
    if value == 0:
        return "0"
    if unit == "ops/s":
        return f"{value:,.0f}"
    if abs(value) < 1:
        return f"{value:.2g}"
    return f"{value:.3g}" if abs(value) < 1000 else f"{value:,.0f}"


def fmt_tick(value, step, unit):
    """An axis tick: as many decimals as the step needs, so 0, 0.05, 0.1
    never collapse into 0.0, 0.1, 0.1."""
    if unit == "ops/s" or step >= 1:
        return f"{value:,.0f}"
    decimals = max(0, -math.floor(math.log10(step)))
    return f"{value:.{decimals}f}"


def fmt_eng(x):
    """Compact axis label: plain below 1000, otherwise mantissa and power of ten
    with up to three significant figures (1e3, 5e4, 1.31e5)."""
    if x < 1000:
        return f"{x:g}"
    e = math.floor(math.log10(x))
    m = x / 10 ** e
    text = f"{m:.2f}".rstrip("0").rstrip(".")
    return f"{text}e{e}"


def fmt_x(x, dimension="agents"):
    """Axis label for a swept level. Byte sizes use binary units (16 KiB,
    1 GiB); counts use round thousands (5k, 1M) and a separator otherwise."""
    if dimension == "payload_bytes":
        for unit, size in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
            if x >= size and x % size == 0:
                return f"{x // size} {unit}"
        return f"{x:,} B"
    if x >= 1_000_000 and x % 1_000_000 == 0:
        return f"{x // 1_000_000}M"
    if x >= 1000 and x % 1000 == 0:
        return f"{x // 1000}k"
    return f"{x:,}"


# --- Layout (px) --------------------------------------------------------------
PANEL_W, PANEL_H = 560, 240  # one metric per panel, title strip and axis included
COLS = 2
PAD, GAP_X, GAP_Y = 36, 56, 36
AXIS_L, AXIS_B = 72, 32  # room for y tick labels / one row of x tick labels
TITLE_H = 40  # panel title strip above the plot area
HEAD = 104  # title, subtitle and legend above the first row
AXIS = "#8a8985"  # the two axis lines, a step darker than the grid
STROKE = 3  # series line width


def x_scale(levels, width):
    """Log x scale over the swept levels (they are geometric)."""
    lo, hi = min(levels), max(levels)
    if lo <= 0 or lo == hi:
        span = (hi - lo) or 1
        return lambda x: width * (x - lo) / span
    llo, lhi = math.log10(lo), math.log10(hi)
    return lambda x: width * (math.log10(x) - llo) / (lhi - llo)


def log_ticks(lo, hi):
    """Decade ticks covering [lo, hi], for a log y axis."""
    lo_exp = math.floor(math.log10(lo)) if lo > 0 else 0
    hi_exp = math.ceil(math.log10(hi)) if hi > 0 else 1
    if hi_exp == lo_exp:
        hi_exp += 1
    return [10.0 ** e for e in range(lo_exp, hi_exp + 1)]


def render(backend, op, dimension, held, group, out_path):
    """Writes one SVG: a panel per metric, direct and dapr curves in each."""
    metrics = list(METRICS)
    levels = sorted({x for pts in group.values() for x, _ in pts})
    tasks = sorted({r["tasks"] for pts in group.values() for _, r in pts})
    rows = math.ceil(len(metrics) / COLS)
    width = PAD * 2 + COLS * PANEL_W + (COLS - 1) * GAP_X
    height = PAD + HEAD + rows * PANEL_H + (rows - 1) * GAP_Y + PAD
    dim_label = DIMENSIONS[dimension]["label"]
    tasks_txt = (f"{tasks[0]:,} tasks per step" if len(tasks) == 1
                 else f"{tasks[0]:,} to {tasks[-1]:,} tasks per step")

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="system-ui, -apple-system, sans-serif">',
        f'<rect width="{width}" height="{height}" fill="{SURFACE}"/>',
        # Title: what is compared. Subtitle: what moves along x, what is held, how much work.
        f'<text x="{width / 2:.1f}" y="{PAD + 10}" font-size="24" font-weight="600" fill="{INK}" text-anchor="middle">'
        f'{esc(BACKEND_NAMES.get(backend, backend))} {esc(OP_NAMES.get(op, op))}: '
        f'<tspan fill="{RGB["direct"]}">direct</tspan> vs <tspan fill="{RGB["dapr"]}">Dapr</tspan></text>',
        f'<text x="{width / 2:.1f}" y="{PAD + 38}" font-size="15" fill="{INK_MUTED}" text-anchor="middle">'
        f'x axis: {esc(dim_label)}'
        + (' in bytes' if dimension == "payload_bytes" else '')
        + f' (log scale) · held fixed: {esc(held_text(dimension, held))} · {tasks_txt}</text>',
    ]
    # Legend, centred under the subtitle. Dapr is dashed as well as orange, so
    # the two stay tellable where they overlap.
    key_w, gap, entry_gap = 30, 9, 44
    text_w = {"direct": 48, "dapr": 40}
    total = sum(key_w + gap + text_w[m] for m in MODES) + entry_gap * (len(MODES) - 1)
    lx = width / 2 - total / 2
    ly = PAD + 64
    for mode in MODES:
        svg.append(f'<line x1="{lx:.1f}" y1="{ly}" x2="{lx + key_w:.1f}" y2="{ly}" {stroke_attrs(mode)}/>')
        svg.append(f'<circle cx="{lx + key_w / 2:.1f}" cy="{ly}" r="5" fill="{RGB[mode]}" stroke="{SURFACE}" stroke-width="2"/>')
        svg.append(f'<text x="{lx + key_w + gap:.1f}" y="{ly + 5}" font-size="14" fill="{INK}">{mode}</text>')
        lx += key_w + gap + text_w[mode] + entry_gap

    plot_w = PANEL_W - AXIS_L
    plot_h = PANEL_H - TITLE_H - AXIS_B
    sx = x_scale(levels, plot_w)

    for n, metric in enumerate(metrics):
        title, unit, scale, get, higher_better = METRICS[metric]
        ox = PAD + (n % COLS) * (PANEL_W + GAP_X) + AXIS_L
        oy = PAD + HEAD + (n // COLS) * (PANEL_H + GAP_Y) + TITLE_H
        curves = {
            mode: [(x, get(r) * scale) for x, r in group.get(mode, [])] for mode in MODES
        }
        values = [v for pts in curves.values() for _, v in pts]
        peak = max(values, default=0)
        # a saturated path can hold a call for tens of seconds; read that in
        # seconds rather than five-digit milliseconds
        if unit == "ms" and peak >= 5000:
            unit = "s"
            curves = {m: [(x, v / 1000) for x, v in pts] for m, pts in curves.items()}
            peak /= 1000

        axis_max, ticks = nice_ticks(peak)
        step = ticks[1] - ticks[0]
        sy = lambda v: oy + plot_h - plot_h * (v / axis_max)  # noqa: E731

        better = "higher is better" if higher_better else "lower is better"
        svg.append(
            f'<text x="{ox - AXIS_L}" y="{oy - 20}" font-size="16" font-weight="600" fill="{INK}">'
            f'{esc(title)} <tspan font-weight="400" fill="{INK_MUTED}">· {unit} · {better}</tspan></text>'
        )
        # Gridlines first, so marks paint over them.
        for t in ticks:
            y = sy(t)
            svg.append(f'<line x1="{ox}" y1="{y:.1f}" x2="{ox + plot_w}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
            svg.append(f'<text x="{ox - 10}" y="{y + 4.5:.1f}" font-size="13" fill="{INK}" text-anchor="end">{fmt_tick(t, step, unit)}</text>')
        # Tick labels are compact exponentials (1e3, 5e4), short enough for one row.
        for x in levels:
            px = ox + sx(x)
            svg.append(f'<line x1="{px:.1f}" y1="{oy + plot_h}" x2="{px:.1f}" y2="{oy + plot_h + 5}" stroke="{AXIS}" stroke-width="1.5"/>')
            svg.append(f'<text x="{px:.1f}" y="{oy + plot_h + 21}" font-size="13" fill="{INK}" text-anchor="middle">{fmt_eng(x)}</text>')
        # The two axes, drawn after the grid so they sit on top of it.
        svg.append(f'<line x1="{ox}" y1="{oy}" x2="{ox}" y2="{oy + plot_h}" stroke="{AXIS}" stroke-width="1.5"/>')
        svg.append(f'<line x1="{ox}" y1="{oy + plot_h}" x2="{ox + plot_w}" y2="{oy + plot_h}" stroke="{AXIS}" stroke-width="1.5"/>')

        # Lines, then markers with a surface ring. Values live in summary.md;
        # per-point labels collided more than they informed.
        for mode in MODES:
            pts = curves[mode]
            if len(pts) < 2:
                continue
            path = " ".join(f'{"M" if i == 0 else "L"}{ox + sx(x):.1f},{sy(v):.1f}' for i, (x, v) in enumerate(pts))
            svg.append(f'<path d="{path}" fill="none" {stroke_attrs(mode)}/>')
        for mode in MODES:
            for x, v in curves[mode]:
                svg.append(f'<circle cx="{ox + sx(x):.1f}" cy="{sy(v):.1f}" r="5" fill="{RGB[mode]}" stroke="{SURFACE}" stroke-width="2"/>')

    svg.append("</svg>")
    with open(out_path, "w") as fh:
        fh.write("\n".join(svg))


def stroke_attrs(mode):
    """Series stroke: solid for direct, dashed for Dapr, both bold."""
    dash = ' stroke-dasharray="9 6"' if mode == "dapr" else ""
    return f'stroke="{RGB[mode]}" stroke-width="{STROKE}" stroke-linejoin="round" stroke-linecap="round"{dash}'


# --- Summary table ------------------------------------------------------------

TABLE_COLS = [
    ("p50 lat", "latency_p50"),
    ("p99 lat", "latency_p99"),
    ("thr", "throughput"),
    ("err", "errors"),
    ("makespan", "makespan"),
]


def summary_rows(dimension, group):
    """Yields one Markdown row per level: direct, dapr and the ratio per column."""
    by_x = defaultdict(dict)
    for mode in MODES:
        for x, r in group.get(mode, []):
            by_x[x][mode] = r
    rows = []
    for x in sorted(by_x, key=lambda x: (x == 0, x)):
        cells = [fmt_x(x, dimension)]
        for _, metric in TABLE_COLS:
            _, unit, scale, get, higher_better = TABLE_METRICS[metric]
            d = by_x[x].get("direct")
            p = by_x[x].get("dapr")
            dv = get(d) * scale if d else None
            pv = get(p) * scale if p else None
            cells.append(fmt(dv, unit) if d else "-")
            cells.append(fmt(pv, unit) if p else "-")
            if d and p and dv and pv:
                ratio = dv / pv if higher_better else pv / dv
                cells.append(f"{ratio:.2f}x")
            else:
                cells.append("-")
        rows.append(cells)
    return rows


def write_summary(backend, tables, out_path):
    lines = [f"# {backend}: direct vs Dapr", ""]
    lines.append("Latency in ms, throughput in successful ops/s, errors in % of operations, "
                 "makespan in seconds for the whole step (all ops). The ratio is Dapr/direct for "
                 "latency, errors and makespan and direct/Dapr for throughput, so above 1.0 always "
                 "means Dapr is worse.")
    for (op, dimension, held), rows in tables:
        others = [d for d in DIMENSIONS if d != dimension]
        held_txt = ", ".join(f"{d}={v}" for d, v in zip(others, held))
        lines += ["", f"## {op} vs {DIMENSIONS[dimension]['label']} ({held_txt})", ""]
        header = [DIMENSIONS[dimension]["label"]]
        for name, _ in TABLE_COLS:
            header += [f"{name} direct", f"{name} dapr", "ratio"]
        lines.append("| " + " | ".join(header) + " |")
        lines.append("|" + "|".join(" ---: " for _ in header) + "|")
        for cells in rows:
            lines.append("| " + " | ".join(cells) + " |")
    with open(out_path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    return "\n".join(lines)


# --- Main --------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="+", help="benchmark traces (.jsonl) to chart")
    ap.add_argument("-o", "--out-dir", default="plots", help="directory for the SVGs (default: plots)")
    ap.add_argument("-q", "--quiet", action="store_true", help="do not print the summary tables")
    args = ap.parse_args()

    records = load(args.files)
    if not records:
        sys.exit("no step records found in: " + ", ".join(args.files))

    backends = sorted({r["backend"] for r in records})
    written = []
    for backend in backends:
        out_dir = os.path.join(args.out_dir, backend)
        os.makedirs(out_dir, exist_ok=True)
        mine = [r for r in records if r["backend"] == backend]
        tables = []
        for dimension in DIMENSIONS:
            groups = series(mine, dimension)
            per_op = defaultdict(int)
            for op, _ in groups:
                per_op[op] += 1
            for (op, held), group in sorted(groups.items()):
                name = f"{op}_vs_{DIMENSIONS[dimension]['slug']}"
                # several load profiles for one dimension (a grid run, or old
                # and new traces measured under different profiles) get named
                # apart by what they hold, e.g. write_vs_payload_at_rate0_users1
                if per_op[op] > 1:
                    others = [d for d in DIMENSIONS if d != dimension]
                    name += "_at_" + "_".join(f"{DIMENSIONS[d]['slug']}{v}" for d, v in zip(others, held))
                path = os.path.join(out_dir, name + ".svg")
                render(backend, op, dimension, held, group, path)
                written.append(path)
                tables.append(((op, dimension, held), summary_rows(dimension, group)))
        text = write_summary(backend, tables, os.path.join(out_dir, "summary.md"))
        written.append(os.path.join(out_dir, "summary.md"))
        if not args.quiet:
            print(text)

    for path in written:
        print(f"wrote {path}", file=sys.stderr)


if __name__ == "__main__":
    main()
