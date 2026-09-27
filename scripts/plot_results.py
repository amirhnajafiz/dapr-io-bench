#!/usr/bin/env python3
"""Charts benchmark traces (traces/*.jsonl) as SVG line charts and a summary table.

    scripts/plot_results.py traces/*.jsonl [-o plots/]

One SVG per backend, operation and load dimension, e.g.
plots/redis/write_vs_rps.svg: four panels (p50 latency, p99 latency,
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
        ("rate", {"label": "RPS", "slug": "rps"}),
        ("payload_bytes", {"label": "payload bytes", "slug": "payload"}),
        ("concurrency", {"label": "concurrent users", "slug": "users"}),
    ]
)

# Panels, in display order: (title, unit, scale, extractor, higher_is_better)
METRICS = OrderedDict(
    [
        ("latency_p50", ("p50 latency", "ms", 1e3, lambda r: r["latency"]["p50"], False)),
        ("latency_p99", ("p99 latency", "ms", 1e3, lambda r: r["latency"]["p99"], False)),
        ("throughput", ("throughput", "ops/s", 1.0, lambda r: r["throughput"], True)),
        ("errors", ("error rate", "%", 1.0, lambda r: 100.0 * r["errors"] / r["ops"] if r["ops"] else 0.0, False)),
    ]
)


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
                        rec["backend"], rec["mode"], rec["op"],
                        rec["rate"], rec["payload_bytes"], rec["concurrency"],
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
    if unit == "ops/s":
        return f"{value:,.0f}"
    if unit == "%":
        return f"{value:.1f}"
    if value == 0:
        return "0"
    if value < 1:
        return f"{value:.2f}"
    return f"{value:.1f}" if value < 100 else f"{value:,.0f}"


def fmt_x(x):
    """Axis label for a swept level: round thousands as 5k, anything else
    with a separator, so 1024 stays 1,024 rather than 1.024k."""
    if x >= 1_000_000 and x % 1_000_000 == 0:
        return f"{x // 1_000_000}M"
    if x >= 1000 and x % 1000 == 0:
        return f"{x // 1000}k"
    return f"{x:,}"


# --- Layout (px) --------------------------------------------------------------
PANEL_W, PANEL_H = 440, 290  # one metric per panel
COLS = 2
PAD, GAP_X, GAP_Y = 32, 40, 48
AXIS_L, AXIS_B = 60, 34  # room for y tick labels / x tick labels inside a panel
LABEL_W = 48  # room to the right of the last point for its end label
HEAD = 76  # centred title + legend block above the first row


def x_scale(levels, width):
    """Log x scale over the swept levels (they are geometric), linear if a level
    is zero (a closed-loop rate of 0 cannot sit on a log axis)."""
    lo, hi = min(levels), max(levels)
    if lo <= 0 or lo == hi:
        span = (hi - lo) or 1
        return lambda x: width * (x - lo) / span
    llo, lhi = math.log10(lo), math.log10(hi)
    return lambda x: width * (math.log10(x) - llo) / (lhi - llo)


def render(backend, op, dimension, group, out_path):
    """Writes one SVG: a panel per metric, direct and dapr curves in each."""
    metrics = list(METRICS)
    levels = sorted({x for pts in group.values() for x, _ in pts})
    rows = math.ceil(len(metrics) / COLS)
    width = PAD * 2 + COLS * PANEL_W + (COLS - 1) * GAP_X
    height = PAD + HEAD + rows * PANEL_H + (rows - 1) * GAP_Y + PAD

    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}" font-family="system-ui, -apple-system, sans-serif">',
        f'<rect width="{width}" height="{height}" fill="{SURFACE}"/>',
        f'<text x="{width / 2:.1f}" y="{PAD}" font-size="18" font-weight="600" fill="{INK}" text-anchor="middle">'
        f'{esc(backend)} {esc(op)} <tspan font-weight="400" fill="{INK_MUTED}">vs {esc(DIMENSIONS[dimension]["label"])}</tspan></text>',
    ]
    # Legend, centred under the title: always present for two series, so
    # identity is never colour-alone.
    entry_w, key_w = 90, 18
    lx = width / 2 - (entry_w * len(MODES) - (entry_w - key_w - 8 - 34)) / 2
    ly = PAD + 26
    for mode in MODES:
        svg.append(f'<line x1="{lx:.1f}" y1="{ly}" x2="{lx + key_w:.1f}" y2="{ly}" stroke="{RGB[mode]}" stroke-width="2" stroke-linecap="round"/>')
        svg.append(f'<circle cx="{lx + key_w / 2:.1f}" cy="{ly}" r="4" fill="{RGB[mode]}" stroke="{SURFACE}" stroke-width="2"/>')
        svg.append(f'<text x="{lx + key_w + 8:.1f}" y="{ly + 4}" font-size="12" fill="{INK_MUTED}">{mode}</text>')
        lx += entry_w

    plot_w = PANEL_W - AXIS_L - LABEL_W
    plot_h = PANEL_H - AXIS_B - 20
    sx = x_scale(levels, plot_w)

    for n, metric in enumerate(metrics):
        title, unit, scale, get, higher_better = METRICS[metric]
        ox = PAD + (n % COLS) * (PANEL_W + GAP_X) + AXIS_L
        oy = PAD + HEAD + (n // COLS) * (PANEL_H + GAP_Y) + 20
        curves = {
            mode: [(x, get(r) * scale) for x, r in group.get(mode, [])] for mode in MODES
        }
        peak = max((v for pts in curves.values() for _, v in pts), default=0)
        axis_max, ticks = nice_ticks(peak)
        sy = lambda v: oy + plot_h - plot_h * (v / axis_max)  # noqa: E731

        better = "higher is better" if higher_better else "lower is better"
        svg.append(
            f'<text x="{ox - AXIS_L}" y="{oy - 10}" font-size="13" font-weight="600" fill="{INK}">'
            f'{esc(title)} <tspan font-weight="400" fill="{INK_MUTED}">({unit}, {better})</tspan></text>'
        )
        # Gridlines first, so marks paint over them.
        for t in ticks:
            y = sy(t)
            svg.append(f'<line x1="{ox}" y1="{y:.1f}" x2="{ox + plot_w}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
            svg.append(f'<text x="{ox - 8}" y="{y + 3.5:.1f}" font-size="10.5" fill="{INK_MUTED}" text-anchor="end">{fmt(t, unit)}</text>')
        for x in levels:
            px = ox + sx(x)
            svg.append(f'<line x1="{px:.1f}" y1="{oy + plot_h}" x2="{px:.1f}" y2="{oy + plot_h + 4}" stroke="{GRID}" stroke-width="1"/>')
            svg.append(f'<text x="{px:.1f}" y="{oy + plot_h + 17}" font-size="10.5" fill="{INK_MUTED}" text-anchor="middle">{fmt_x(x)}</text>')
        svg.append(f'<text x="{ox + plot_w / 2:.1f}" y="{oy + plot_h + 31}" font-size="10.5" fill="{INK_MUTED}" text-anchor="middle">{esc(DIMENSIONS[dimension]["label"])}</text>')

        # Lines, then markers with a surface ring, then one end label per series.
        for mode in MODES:
            pts = curves[mode]
            if not pts:
                continue
            path = " ".join(f'{"M" if i == 0 else "L"}{ox + sx(x):.1f},{sy(v):.1f}' for i, (x, v) in enumerate(pts))
            svg.append(f'<path d="{path}" fill="none" stroke="{RGB[mode]}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')
        for mode in MODES:
            for x, v in curves[mode]:
                svg.append(f'<circle cx="{ox + sx(x):.1f}" cy="{sy(v):.1f}" r="4.5" fill="{RGB[mode]}" stroke="{SURFACE}" stroke-width="2"/>')
        # End labels sit to the right of the last point; when the two series
        # end close together the labels are nudged apart, keeping their order.
        ends = {mode: curves[mode][-1] for mode in MODES if curves[mode]}
        ys = {mode: sy(v) for mode, (_, v) in ends.items()}
        if len(ys) == 2 and abs(ys["direct"] - ys["dapr"]) < 13:
            mid = (ys["direct"] + ys["dapr"]) / 2
            upper = min(ys, key=ys.get)
            for mode in ys:
                ys[mode] = mid - 6.5 if mode == upper else mid + 6.5
        for mode, (x, v) in ends.items():
            svg.append(
                f'<text x="{ox + sx(x) + 9:.1f}" y="{ys[mode] + 4:.1f}" font-size="11" fill="{INK_MUTED}">'
                f'{fmt(v, unit)}</text>'
            )

    svg.append("</svg>")
    with open(out_path, "w") as fh:
        fh.write("\n".join(svg))


# --- Summary table ------------------------------------------------------------

TABLE_COLS = [
    ("p50 lat", "latency_p50"),
    ("p99 lat", "latency_p99"),
    ("thr", "throughput"),
    ("err", "errors"),
]


def summary_rows(dimension, group):
    """Yields one Markdown row per level: direct, dapr and the ratio per column."""
    by_x = defaultdict(dict)
    for mode in MODES:
        for x, r in group.get(mode, []):
            by_x[x][mode] = r
    rows = []
    for x in sorted(by_x):
        cells = [fmt_x(x)]
        for _, metric in TABLE_COLS:
            _, unit, scale, get, higher_better = METRICS[metric]
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
    lines.append("Latency in ms, throughput in successful ops/s, errors in % of operations. "
                 "The ratio is Dapr/direct for latency and errors and direct/Dapr for throughput, "
                 "so above 1.0 always means Dapr is worse.")
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
                # a grid run has several held groups per dimension; name them apart
                if per_op[op] > 1:
                    name += "_" + "_".join(str(v) for v in held)
                path = os.path.join(out_dir, name + ".svg")
                render(backend, op, dimension, group, path)
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
