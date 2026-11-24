from __future__ import annotations


def _dec(s, k):
    d = s["safety_decisions"].get(k)
    return d["count"] if d else 0


def print_comparison(summaries: list[dict]) -> None:
    hdr = ["scenario", "mode", "talk util", "occupancy", "initiated", "answered", "connected",
           "abandoned", "abandon %", "approve", "reduce", "reject", "fallback", "shed", "invariant errs"]
    rows = []
    for s in summaries:
        rows.append([s["scenario"], s["mode"], f"{s['talk_utilization']:.1%}", f"{s['occupancy_incl_wrap']:.1%}",
                     s["calls_initiated"], s["calls_answered"], s["calls_connected"], s["abandoned"],
                     f"{s['abandon_rate']:.2%}", _dec(s, "APPROVE"), _dec(s, "REDUCE"), _dec(s, "REJECT"),
                     _dec(s, "FALLBACK_PROGRESSIVE"), s["shed_calls"], s["invariant_violations"]])
    w = [max(len(str(x)) for x in col) for col in zip(hdr, *rows)]
    line = lambda r: "| " + " | ".join(str(x).ljust(n) for x, n in zip(r, w)) + " |"
    print(line(hdr))
    print("|" + "|".join("-" * (n + 2) for n in w) + "|")
    for r in rows:
        print(line(r))


def print_timeline(summary: dict, timeline: list[dict], log: list[str]) -> None:
    print(f"\n### {summary['scenario']} / {summary['mode']} — {summary.get('description','')}")
    for l in log:
        print("   ", l)
    cols = ["t_min", "agents_logged_in", "connected_now", "util_now", "initiated", "answered", "connected",
            "abandoned", "failed_system", "requested", "approved", "decisions", "health"]
    print(" | ".join(cols))
    for r in timeline:
        dec = ",".join(f"{k[:4]}:{v}" for k, v in sorted(r["decisions"].items()))
        print(" | ".join(str(r[c]) if c != "decisions" else dec for c in cols))
