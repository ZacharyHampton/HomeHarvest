"""
Benchmark + field fill-rate harness for HomeHarvest (not shipped in the package).

Hits the live API. Measures wall time, request count and bytes per scenario, and how often
every output column is filled. Works against any HomeHarvest checkout (pick it with
PYTHONPATH), so an old release and the working tree can be run back to back:

    PYTHONPATH=/path/to/v0.9.0 python scripts/bench.py run --label v0.9.0 --out a.json --sigs a.sigs.json
    PYTHONPATH=.               python scripts/bench.py run --label 0.9.1  --out b.json --sigs b.sigs.json
    python scripts/bench.py compare a.json b.json --sigs a.sigs.json b.sigs.json

Fill rates drift with live data, so `compare` also checks the homes both runs returned
(same property_id): a column that was filled before and is empty now is a regression even
when the overall rate looks fine. Runs that saw a 403 / 429 or a HomeHarvestWarning are
marked tainted and should not be used for fill rates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
import threading
import time
import warnings
from collections import Counter, defaultdict

SCENARIOS = {
    "dallas_sold_200": dict(location="Dallas, TX", listing_type="sold", past_days=30, limit=200),
    "dallas_sold_200_extras": dict(
        location="Dallas, TX", listing_type="sold", past_days=30, limit=200, extra_property_data=True
    ),
    "phoenix_for_sale_200_extras": dict(
        location="Phoenix, AZ", listing_type="for_sale", limit=200, extra_property_data=True
    ),
    "phoenix_for_sale_1000": dict(location="Phoenix, AZ", listing_type="for_sale", limit=1000),
    "dallas_pending_7d_extras": dict(
        location="Dallas, TX", listing_type="pending", past_days=7, extra_property_data=True
    ),
    "austin_open_house": dict(location="Austin, TX", listing_type="for_sale", open_house=True, limit=1000),
    "houston_for_sale_2000": dict(location="Houston, TX", listing_type="for_sale", limit=2000),
    #: not in the default matrix: used to check detail batch sizes on rentals
    "dallas_rent_200_extras": dict(
        location="Dallas, TX", listing_type="for_rent", limit=200, extra_property_data=True
    ),
}
DEFAULT_SCENARIOS = [name for name in SCENARIOS if name != "dallas_rent_200_extras"]

#: columns worth calling out in reports
KEY_COLUMNS = [
    "tax_history", "nearby_schools", "assessed_value", "tax", "property_history", "original_list_price",
    "buyer_agent_name", "buyer_agent_email", "buyer_agent_phones", "buyer_office_name",
    "zip_median_rent", "neighborhood_median_rent", "insurance_rate", "property_tax_rate",
    "monthly_home_insurance", "open_houses", "next_open_house_start", "agent_email", "agent_phones",
    "office_phones", "primary_photo", "alt_photos", "estimated_value", "list_date", "days_on_mls",
]


# ---------------------------------------------------------------- HTTP instrumentation


class Meter:
    """Counts every HTTP request HomeHarvest makes, whatever library it uses."""

    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        self.requests = 0
        self.sent_bytes = 0
        self.body_bytes = 0  #: decoded response bodies
        self.statuses = Counter()
        self.ops = defaultdict(list)  #: operation name -> latencies
        self.in_flight = 0
        self.max_in_flight = 0

    def start(self):
        with self.lock:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)

    def record(self, data, response, seconds):
        try:
            op = json.loads(data).get("operationName") or "?"
        except Exception:
            op = "?"
        body = getattr(response, "content", b"") or b""
        with self.lock:
            self.in_flight -= 1
            self.requests += 1
            self.sent_bytes += len(data or b"")
            self.body_bytes += len(body)
            self.statuses[getattr(response, "status_code", 0)] += 1
            self.ops[op].append(seconds)

    def failed(self):
        with self.lock:
            self.in_flight -= 1
            self.statuses["exception"] += 1


METER = Meter()


def _timed(original):
    def wrapper(*args, **kwargs):
        data = kwargs.get("data")
        if data is None and len(args) > 2:
            data = args[2]
        METER.start()
        t0 = time.perf_counter()
        try:
            response = original(*args, **kwargs)
        except Exception:
            METER.failed()
            raise
        METER.record(data, response, time.perf_counter() - t0)
        return response

    return wrapper


def instrument():
    """Hook the lowest HTTP call HomeHarvest makes (0.9.1+: the transport; 0.9.0: requests.post)."""
    try:
        from homeharvest.core.scrapers import http as transport  # 0.9.1+

        transport.HttpClient.post = _timed(transport.HttpClient.post)
        return "homeharvest.core.scrapers.http.HttpClient.post"
    except ImportError:
        import requests

        requests.post = _timed(requests.post)
        return "requests.post"


# ---------------------------------------------------------------- fill rates


def _is_filled(value) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, tuple, dict, set, str)):
        return len(value) > 0
    try:
        import pandas as pd

        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return True


def _signature(value) -> str | None:
    """Small fingerprint of a cell: list/dict length, else a short hash of the value."""
    if not _is_filled(value):
        return None
    if isinstance(value, (list, tuple, dict)):
        return f"L{len(value)}"
    return hashlib.blake2b(str(value).encode(), digest_size=5).hexdigest()


def fill_report(df) -> dict:
    rows = len(df)
    if not rows:
        return {}
    return {col: round(100.0 * sum(_is_filled(v) for v in df[col]) / rows, 2) for col in df.columns}


def signatures(df) -> dict:
    return {
        str(row["property_id"]): {col: _signature(row[col]) for col in df.columns}
        for _, row in df.iterrows()
    }


# ---------------------------------------------------------------- run


def run(args):
    instrumented = instrument()
    import homeharvest
    from homeharvest import scrape_property

    names = args.scenarios.split(",") if args.scenarios else DEFAULT_SCENARIOS
    out = {
        "label": args.label,
        "homeharvest": homeharvest.__version__,
        "homeharvest_path": homeharvest.__file__,
        "instrumented": instrumented,
        "started": time.strftime("%Y-%m-%d %H:%M:%S"),
        "python": sys.version.split()[0],
        "scenarios": {},
    }
    sigs = {}
    print(f"homeharvest {homeharvest.__version__} from {homeharvest.__file__} (hook: {instrumented})", flush=True)

    for name in names:
        params = dict(SCENARIOS[name])
        runs = []
        for i in range(args.runs):
            METER.reset()
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                t0 = time.perf_counter()
                error = None
                try:
                    df = scrape_property(**params)
                except Exception as exc:  # keep going, record it
                    error = f"{type(exc).__name__}: {exc}"
                    df = None
                seconds = time.perf_counter() - t0
            hh_warnings = [str(w.message)[:300] for w in caught if "HomeHarvest" in type(w.message).__name__]
            bad_status = {str(k): v for k, v in METER.statuses.items() if k != 200}
            rec = {
                "seconds": round(seconds, 3),
                "rows": 0 if df is None else len(df),
                "requests": METER.requests,
                "sent_bytes": METER.sent_bytes,
                "body_bytes": METER.body_bytes,
                "max_in_flight": METER.max_in_flight,
                "statuses": {str(k): v for k, v in METER.statuses.items()},
                "op_latency_median": {
                    op: round(statistics.median(v), 3) for op, v in METER.ops.items()
                },
                "op_counts": {op: len(v) for op, v in METER.ops.items()},
                "warnings": hh_warnings,
                "error": error,
                "tainted": bool(error or hh_warnings or bad_status),
            }
            if df is not None and len(df):
                rec["fill"] = fill_report(df)
                if i == 0 and args.sigs:
                    sigs[name] = signatures(df)
            runs.append(rec)
            print(
                f"{name:30s} run {i + 1}: {seconds:6.2f}s rows={rec['rows']:5d} req={rec['requests']:3d} "
                f"body={rec['body_bytes'] / 1e6:6.2f}MB inflight<={rec['max_in_flight']} "
                f"status={rec['statuses']}{' TAINTED' if rec['tainted'] else ''}"
                + (f" warn={hh_warnings[0][:120]!r}" if hh_warnings else "")
                + (f" error={error[:160]!r}" if error else ""),
                flush=True,
            )
            if args.pause:
                time.sleep(args.pause)
        out["scenarios"][name] = {"params": params, "runs": runs}

    with open(args.out, "w") as fh:
        json.dump(out, fh, indent=1, default=str)
    if args.sigs:
        with open(args.sigs, "w") as fh:
            json.dump(sigs, fh)
    print(f"saved {args.out}" + (f" and {args.sigs}" if args.sigs else ""))


# ---------------------------------------------------------------- compare


def _median(runs, key):
    values = [r[key] for r in runs if r.get(key) is not None]
    return statistics.median(values) if values else None


def _mean_fill(runs) -> dict:
    """Average fill rate per column over clean runs (all runs if none are clean)."""
    clean = [r for r in runs if r.get("fill") and not r["tainted"]] or [r for r in runs if r.get("fill")]
    columns = set().union(*(r["fill"].keys() for r in clean)) if clean else set()
    return {c: round(statistics.mean(r["fill"].get(c, 0.0) for r in clean), 2) for c in columns}


def compare(args):
    a, b = (json.load(open(p)) for p in (args.a, args.b))
    sig_a, sig_b = (json.load(open(p)) for p in args.sigs) if args.sigs else ({}, {})
    print(f"A = {a['label']} ({a['homeharvest']}, {a['started']})   B = {b['label']} ({b['homeharvest']}, {b['started']})\n")
    print("| scenario | rows A/B | time A -> B (median s) | requests A -> B | body MB A -> B | tainted A/B |")
    print("|---|---|---|---|---|---|")
    for name in a["scenarios"]:
        if name not in b["scenarios"]:
            continue
        ra, rb = a["scenarios"][name]["runs"], b["scenarios"][name]["runs"]
        ta, tb = _median(ra, "seconds"), _median(rb, "seconds")
        print(
            f"| {name} | {_median(ra, 'rows'):.0f}/{_median(rb, 'rows'):.0f} | {ta:.2f} -> {tb:.2f} ({ta / tb:.1f}x) | "
            f"{_median(ra, 'requests'):.0f} -> {_median(rb, 'requests'):.0f} | "
            f"{_median(ra, 'body_bytes') / 1e6:.2f} -> {_median(rb, 'body_bytes') / 1e6:.2f} | "
            f"{sum(r['tainted'] for r in ra)}/{sum(r['tainted'] for r in rb)} |"
        )

    regressions = []
    for name in a["scenarios"]:
        if name not in b["scenarios"]:
            continue
        fa, fb = _mean_fill(a["scenarios"][name]["runs"]), _mean_fill(b["scenarios"][name]["runs"])
        sa, sb = sig_a.get(name, {}), sig_b.get(name, {})
        common = sorted(set(sa) & set(sb))
        print(f"\n### {name}: fill rate % (all rows A -> B; same {len(common)} homes A -> B; emptied/changed on same homes)")
        print("| column | all A | all B | same A | same B | emptied | changed |")
        print("|---|---|---|---|---|---|---|")
        for col in sorted(set(fa) | set(fb), key=lambda c: (c not in KEY_COLUMNS, c)):
            if common:
                same_a = 100.0 * sum(sa[i].get(col) is not None for i in common) / len(common)
                same_b = 100.0 * sum(sb[i].get(col) is not None for i in common) / len(common)
                emptied = sum(sa[i].get(col) is not None and sb[i].get(col) is None for i in common)
                changed = sum(
                    sa[i].get(col) is not None and sb[i].get(col) is not None and sa[i][col] != sb[i][col]
                    for i in common
                )
            else:
                same_a = same_b = float("nan")
                emptied = changed = 0
            drop = (same_a - same_b) if common else (fa.get(col, 0) - fb.get(col, 0))
            flag = drop > 1.0
            if flag:
                regressions.append((name, col, drop))
            if args.all or col in KEY_COLUMNS or flag or emptied:
                print(
                    f"| {col}{' **REGRESSION**' if flag else ''} | {fa.get(col, 0):.1f} | {fb.get(col, 0):.1f} | "
                    f"{same_a:.1f} | {same_b:.1f} | {emptied} | {changed} |"
                )
    print("\nRegressions (> 1 point drop on the same homes):", regressions or "none")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_run = sub.add_parser("run")
    p_run.add_argument("--label", required=True)
    p_run.add_argument("--runs", type=int, default=3)
    p_run.add_argument("--scenarios", help=f"comma list; default: {','.join(DEFAULT_SCENARIOS)}")
    p_run.add_argument("--out", required=True)
    p_run.add_argument("--sigs", help="also save per-home cell fingerprints (first run) for compare")
    p_run.add_argument("--pause", type=float, default=1.0, help="seconds between runs (politeness)")
    p_cmp = sub.add_parser("compare")
    p_cmp.add_argument("a")
    p_cmp.add_argument("b")
    p_cmp.add_argument("--sigs", nargs=2, metavar=("A_SIGS", "B_SIGS"))
    p_cmp.add_argument("--all", action="store_true", help="print every column, not just key ones")
    args = parser.parse_args()
    run(args) if args.cmd == "run" else compare(args)


if __name__ == "__main__":
    main()
