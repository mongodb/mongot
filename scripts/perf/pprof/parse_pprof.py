#!/usr/bin/env python3
"""
Parse and analyze pprof protobuf profiles (CPU, heap, etc.).

Run via Bazel:
    bazel run //scripts/perf/pprof:parse_pprof -- profile.pb.gz
    bazel run //scripts/perf/pprof:parse_pprof -- profile.pb.gz --top 50
    bazel run //scripts/perf/pprof:parse_pprof -- profile.pb.gz --cum
    bazel run //scripts/perf/pprof:parse_pprof -- profile.pb.gz --filter "Quantiz"
    bazel run //scripts/perf/pprof:parse_pprof -- profile.pb.gz --stacks "scorer"
    bazel run //scripts/perf/pprof:parse_pprof -- profile.pb.gz --diff profile2.pb
    bazel run //scripts/perf/pprof:parse_pprof -- profile.pb.gz --raw
"""

import argparse
import gzip
import re
import sys
from collections import defaultdict
from pathlib import Path

from scripts.perf.pprof import profile_pb2


def load_profile(path: str) -> profile_pb2.Profile:
    """Load a pprof profile from .pb or .pb.gz file."""
    with open(path, "rb") as f:
        data = f.read()
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    prof = profile_pb2.Profile()
    prof.ParseFromString(data)
    return prof


def func_name(prof: profile_pb2.Profile, func_id: int) -> str:
    """Get the function name for a function ID."""
    for fn in prof.function:
        if fn.id == func_id:
            return prof.string_table[fn.name]
    return f"<unknown:{func_id}>"


def build_func_index(prof: profile_pb2.Profile) -> dict:
    """Build function ID -> Function lookup."""
    return {fn.id: fn for fn in prof.function}


def build_location_index(prof: profile_pb2.Profile) -> dict:
    """Build location ID -> Location lookup."""
    return {loc.id: loc for loc in prof.location}


def location_func_name(
    prof: profile_pb2.Profile, loc_id: int, loc_idx: dict, func_idx: dict
) -> str:
    """Get the function name for a location ID (uses first line entry)."""
    loc = loc_idx.get(loc_id)
    if loc and loc.line:
        fn = func_idx.get(loc.line[0].function_id)
        if fn:
            return prof.string_table[fn.name]
    return f"<loc:{loc_id}>"


def location_full_name(
    prof: profile_pb2.Profile, loc_id: int, loc_idx: dict, func_idx: dict
) -> str:
    """Get function name with file and line info."""
    loc = loc_idx.get(loc_id)
    if loc and loc.line:
        line = loc.line[0]
        fn = func_idx.get(line.function_id)
        if fn:
            name = prof.string_table[fn.name]
            filename = prof.string_table[fn.filename] if fn.filename else ""
            if filename and line.line:
                short = filename.rsplit("/", 1)[-1] if "/" in filename else filename
                return f"{name} ({short}:{line.line})"
            return name
    return f"<loc:{loc_id}>"


def compute_flat_cum(prof: profile_pb2.Profile, value_idx: int = 0) -> tuple:
    """Compute flat and cumulative values per function name.

    flat = value directly attributed to this function (leaf of stack)
    cum = value for all stacks containing this function
    """
    flat = defaultdict(int)
    cum = defaultdict(int)
    loc_idx = build_location_index(prof)
    func_idx = build_func_index(prof)

    for sample in prof.sample:
        if value_idx >= len(sample.value):
            continue
        val = sample.value[value_idx]
        if val == 0:
            continue

        # Flat: only the leaf (first location_id = top of stack)
        if sample.location_id:
            leaf_name = location_func_name(prof, sample.location_id[0], loc_idx, func_idx)
            flat[leaf_name] += val

        # Cumulative: every function in the stack (deduplicated per sample)
        seen = set()
        for lid in sample.location_id:
            name = location_func_name(prof, lid, loc_idx, func_idx)
            if name not in seen:
                cum[name] += val
                seen.add(name)

    return flat, cum


def format_value(val: int, unit: str) -> str:
    """Format a value with its unit in human-readable form."""
    u = unit.lower()
    if u in ("nanoseconds", "nanosecond", "ns"):
        if abs(val) >= 1_000_000_000:
            return f"{val / 1e9:.2f}s"
        if abs(val) >= 1_000_000:
            return f"{val / 1e6:.1f}ms"
        if abs(val) >= 1_000:
            return f"{val / 1e3:.1f}us"
        return f"{val}ns"
    if u in ("microseconds", "microsecond", "us"):
        if abs(val) >= 1_000_000:
            return f"{val / 1e6:.2f}s"
        if abs(val) >= 1_000:
            return f"{val / 1e3:.1f}ms"
        return f"{val}us"
    if u in ("milliseconds", "millisecond", "ms"):
        if abs(val) >= 1000:
            return f"{val / 1e3:.2f}s"
        return f"{val}ms"
    if u in ("bytes", "byte"):
        if abs(val) >= 1_073_741_824:
            return f"{val / (1024**3):.1f}GB"
        if abs(val) >= 1_048_576:
            return f"{val / (1024**2):.1f}MB"
        if abs(val) >= 1024:
            return f"{val / 1024:.1f}KB"
        return f"{val}B"
    if u == "count":
        if abs(val) >= 1_000_000:
            return f"{val / 1e6:.1f}M"
        if abs(val) >= 1_000:
            return f"{val / 1e3:.1f}K"
        return str(val)
    return str(val)


def get_unit(prof: profile_pb2.Profile, value_idx: int) -> str:
    if value_idx < len(prof.sample_type):
        return prof.string_table[prof.sample_type[value_idx].unit]
    return "samples"


def print_profile_info(prof: profile_pb2.Profile):
    """Print basic profile metadata."""
    print("=== Profile Info ===")
    print(f"  Samples:   {len(prof.sample)}")
    print(f"  Locations: {len(prof.location)}")
    print(f"  Functions: {len(prof.function)}")
    print(f"  Mappings:  {len(prof.mapping)}")
    if prof.duration_nanos:
        print(f"  Duration:  {format_value(prof.duration_nanos, 'nanoseconds')}")
    if prof.period and prof.period_type.type:
        unit = prof.string_table[prof.period_type.unit]
        typ = prof.string_table[prof.period_type.type]
        print(f"  Period:    {prof.period} {unit} ({typ})")

    print("  Sample types:")
    for i, st in enumerate(prof.sample_type):
        typ = prof.string_table[st.type]
        unit = prof.string_table[st.unit]
        print(f"    [{i}] {typ} ({unit})")
    print()


def print_top(
    prof: profile_pb2.Profile,
    n: int = 30,
    sort_cum: bool = False,
    filter_str: str = "",
    value_idx: int = 0,
    use_regex: bool = False,
):
    """Print top functions by flat or cumulative value."""
    flat, cum = compute_flat_cum(prof, value_idx)
    unit = get_unit(prof, value_idx)
    total = sum(flat.values())
    all_funcs = set(flat.keys()) | set(cum.keys())

    if filter_str:
        if use_regex:
            pattern = re.compile(filter_str, re.IGNORECASE)
            all_funcs = {f for f in all_funcs if pattern.search(f)}
        else:
            all_funcs = {f for f in all_funcs if filter_str.lower() in f.lower()}

    if sort_cum:
        ranked = sorted(all_funcs, key=lambda f: cum.get(f, 0), reverse=True)
    else:
        ranked = sorted(all_funcs, key=lambda f: flat.get(f, 0), reverse=True)

    ranked = ranked[:n]

    sort_label = "cum" if sort_cum else "flat"
    print(f"=== Top {len(ranked)} by {sort_label} ({unit}) ===")
    if filter_str:
        print(f"  Filter: '{filter_str}'")
    print(f"  Total: {format_value(total, unit)}")
    print()

    max_flat_w = max((len(format_value(flat.get(f, 0), unit)) for f in ranked), default=5)
    max_cum_w = max((len(format_value(cum.get(f, 0), unit)) for f in ranked), default=5)
    max_flat_w = max(max_flat_w, 4)
    max_cum_w = max(max_cum_w, 3)

    print(f"  {'flat':>{max_flat_w}}  {'flat%':>6}  {'cum':>{max_cum_w}}  {'cum%':>6}  function")
    print(f"  {'─' * max_flat_w}  {'─' * 6}  {'─' * max_cum_w}  {'─' * 6}  {'─' * 40}")

    for f in ranked:
        fv = flat.get(f, 0)
        cv = cum.get(f, 0)
        fpct = (fv / total * 100) if total else 0
        cpct = (cv / total * 100) if total else 0
        print(
            f"  {format_value(fv, unit):>{max_flat_w}}  {fpct:5.1f}%  "
            f"{format_value(cv, unit):>{max_cum_w}}  {cpct:5.1f}%  {f}"
        )
    print()


def print_stacks(
    prof: profile_pb2.Profile, filter_str: str, value_idx: int = 0, max_stacks: int = 20
):
    """Print call stacks containing a function matching filter_str."""
    unit = get_unit(prof, value_idx)
    loc_idx = build_location_index(prof)
    func_idx = build_func_index(prof)

    matching = []
    for sample in prof.sample:
        if value_idx >= len(sample.value) or sample.value[value_idx] == 0:
            continue
        for lid in sample.location_id:
            name = location_func_name(prof, lid, loc_idx, func_idx)
            if filter_str.lower() in name.lower():
                matching.append(sample)
                break

    matching.sort(key=lambda s: s.value[value_idx], reverse=True)

    print(f"=== Call stacks containing '{filter_str}' ({len(matching)} samples) ===")
    print()

    for i, sample in enumerate(matching[:max_stacks]):
        val = sample.value[value_idx]
        print(f"  Stack #{i + 1}  value={format_value(val, unit)}")
        for j, lid in enumerate(reversed(list(sample.location_id))):
            name = location_full_name(prof, lid, loc_idx, func_idx)
            marker = " >>>" if filter_str.lower() in name.lower() else "    "
            depth = j
            print(f"  {marker} {'  ' * depth}{name}")
        print()


def print_diff(
    prof1: profile_pb2.Profile, prof2: profile_pb2.Profile, n: int = 30, value_idx: int = 0
):
    """Compare two profiles and show the differences."""
    flat1, cum1 = compute_flat_cum(prof1, value_idx)
    flat2, cum2 = compute_flat_cum(prof2, value_idx)
    unit = get_unit(prof1, value_idx)

    total1 = sum(flat1.values())
    total2 = sum(flat2.values())

    all_funcs = set(flat1.keys()) | set(flat2.keys())

    diffs = {}
    pcts1 = {}
    pcts2 = {}
    for f in all_funcs:
        pcts1[f] = (flat1.get(f, 0) / total1 * 100) if total1 else 0
        pcts2[f] = (flat2.get(f, 0) / total2 * 100) if total2 else 0
        diffs[f] = pcts2[f] - pcts1[f]

    ranked = sorted(all_funcs, key=lambda f: abs(diffs[f]), reverse=True)[:n]

    print("=== Diff: profile2 - profile1 (by flat% change) ===")
    print(f"  Profile 1 total: {format_value(total1, unit)}")
    print(f"  Profile 2 total: {format_value(total2, unit)}")
    print()
    print("  Perf tests run queries in a loop, so absolute cycles are not comparable")
    print("  across profiles — a faster patch processes more queries and accumulates")
    print("  more total CPU. Read flat1%/flat2%/diff% (primary columns below). The")
    print("  flat1/flat2 absolute columns are trailing context, useful only when you")
    print("  know both profiles cover identical work. Also watch for new hotspots")
    print("  (0% → N%) and call-stack shape changes (use --stacks to inspect).")
    print()
    print(f"  {'flat1%':>6}  {'flat2%':>6}  {'diff%':>7}  {'flat1':>8}  {'flat2':>8}  function")
    print(f"  {'─' * 6}  {'─' * 6}  {'─' * 7}  {'─' * 8}  {'─' * 8}  {'─' * 40}")

    for f in ranked:
        f1 = flat1.get(f, 0)
        f2 = flat2.get(f, 0)
        p1 = pcts1[f]
        p2 = pcts2[f]
        d = diffs[f]
        sign = "+" if d > 0 else ""
        print(
            f"  {p1:5.1f}%  {p2:5.1f}%  {sign}{d:5.1f}%  "
            f"{format_value(f1, unit):>8}  {format_value(f2, unit):>8}  {f}"
        )
    print()


def print_raw(prof: profile_pb2.Profile, value_idx: int = 0, max_samples: int = 50):
    """Dump raw samples."""
    unit = get_unit(prof, value_idx)
    loc_idx = build_location_index(prof)
    func_idx = build_func_index(prof)

    sorted_samples = sorted(
        prof.sample,
        key=lambda s: s.value[value_idx] if value_idx < len(s.value) else 0,
        reverse=True,
    )

    print(f"=== Raw samples (top {min(max_samples, len(sorted_samples))}) ===")
    print()

    for i, sample in enumerate(sorted_samples[:max_samples]):
        vals = ", ".join(str(v) for v in sample.value)
        print(f"  Sample #{i + 1}  values=[{vals}]")
        for lid in sample.location_id:
            name = location_full_name(prof, lid, loc_idx, func_idx)
            print(f"    {name}")
        print()


def main():
    parser = argparse.ArgumentParser(
        description="Parse and analyze pprof protobuf profiles",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("profile", help="Path to .pb or .pb.gz pprof file")
    parser.add_argument("--top", type=int, default=30, help="Number of top functions (default: 30)")
    parser.add_argument("--cum", action="store_true", help="Sort by cumulative instead of flat")
    parser.add_argument(
        "--filter",
        type=str,
        default="",
        help="Filter functions by substring (or regex with --regex)",
    )
    parser.add_argument(
        "--regex", action="store_true", help="Treat --filter as regex instead of substring"
    )
    parser.add_argument(
        "--stacks", type=str, default="", help="Show call stacks containing function"
    )
    parser.add_argument("--diff", type=str, default="", help="Path to second profile for diff")
    parser.add_argument("--raw", action="store_true", help="Dump raw samples")
    parser.add_argument("--value", type=int, default=0, help="Sample value index (default: 0)")
    parser.add_argument(
        "--max-stacks", type=int, default=20, help="Max stacks to show (default: 20)"
    )
    args = parser.parse_args()

    prof = load_profile(args.profile)
    print_profile_info(prof)

    if args.diff:
        prof2 = load_profile(args.diff)
        print_diff(prof, prof2, n=args.top, value_idx=args.value)
    elif args.stacks:
        print_stacks(prof, args.stacks, value_idx=args.value, max_stacks=args.max_stacks)
    elif args.raw:
        print_raw(prof, value_idx=args.value)
    else:
        print_top(
            prof,
            n=args.top,
            sort_cum=args.cum,
            filter_str=args.filter,
            value_idx=args.value,
            use_regex=args.regex,
        )


if __name__ == "__main__":
    main()
