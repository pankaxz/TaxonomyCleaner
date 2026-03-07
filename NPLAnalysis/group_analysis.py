#!/usr/bin/env python3
"""
NPL Per-Group Log-Transform Outlier Analysis

Two-layer detection:
  1. Global:    z-score each connected skill against the full population
  2. Per-group: z-score each skill against its group peers

Combined suspicion score = α × |global_z| + β × |group_z|
Skills that rank high on BOTH are almost certainly false positives.
Skills high globally but low in-group (Python, Java) are legitimate hubs.
"""

import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


def load_nodes(universe_path: Path) -> dict:
    with open(universe_path) as f:
        data = json.load(f)
    return data["nodes"]


def _median_absolute_deviation(values: list[float]) -> float:
    """MAD with normal-consistency constant (1.4826) to estimate σ."""
    med = statistics.median(values)
    abs_devs = [abs(v - med) for v in values]
    return 1.4826 * statistics.median(abs_devs)


def _fit_distribution(log_values: list[float], use_mad: bool) -> tuple[float, float]:
    """Return (center, spread) using either mean/stdev or median/MAD."""
    if use_mad:
        center = statistics.median(log_values)
        spread = _median_absolute_deviation(log_values)
    else:
        center = statistics.mean(log_values)
        spread = statistics.stdev(log_values)
    return center, spread


def _z_score(value: float, center: float, spread: float) -> float | None:
    if spread == 0:
        return None
    return (value - center) / spread


# ── Core analysis ────────────────────────────────────────────────────

def analyze(
    nodes: dict,
    sigma_threshold: float = 2.0,
    alpha: float = 0.4,
    beta: float = 0.6,
    min_group_size: int = 5,
    use_mad: bool = True,
) -> dict:
    connected = {
        name: info for name, info in nodes.items() if info["total_count"] > 0
    }

    # ── 1. Global distribution ───────────────────────────────────────
    log_counts = {name: math.log(info["total_count"]) for name, info in connected.items()}
    all_log_values = list(log_counts.values())
    global_center, global_spread = _fit_distribution(all_log_values, use_mad)

    # ── 2. Per-group distributions ───────────────────────────────────
    groups: dict[str, list[str]] = defaultdict(list)
    for name, info in connected.items():
        groups[info["group"]].append(name)

    group_stats: dict[str, dict] = {}
    for group_name, members in groups.items():
        if len(members) >= min_group_size:
            group_log_values = [log_counts[m] for m in members]
            center, spread = _fit_distribution(group_log_values, use_mad)
            group_stats[group_name] = {
                "size": len(members),
                "center": round(center, 4),
                "spread": round(spread, 4),
                "sufficient": True,
            }
        else:
            group_stats[group_name] = {
                "size": len(members),
                "center": None,
                "spread": None,
                "sufficient": False,
            }

    # ── 3. Score every connected skill ───────────────────────────────
    scored = []
    for name, info in connected.items():
        log_c = log_counts[name]
        g_z = _z_score(log_c, global_center, global_spread)
        if g_z is None:
            continue

        grp = info["group"]
        gs = group_stats[grp]
        if gs["sufficient"] and gs["spread"] and gs["spread"] > 0:
            grp_z = _z_score(log_c, gs["center"], gs["spread"])
        else:
            grp_z = None

        # Combined suspicion score
        if grp_z is not None:
            suspicion = alpha * abs(g_z) + beta * abs(grp_z)
        else:
            suspicion = abs(g_z)

        scored.append({
            "skill": name,
            "total_count": info["total_count"],
            "log_count": round(log_c, 4),
            "group": grp,
            "super_group": info["super_group"],
            "group_size": gs["size"],
            "global_z": round(g_z, 4),
            "group_z": round(grp_z, 4) if grp_z is not None else None,
            "suspicion_score": round(suspicion, 4),
            "verdict": _verdict(g_z, grp_z, sigma_threshold),
        })

    scored.sort(key=lambda x: x["suspicion_score"], reverse=True)

    flagged = [s for s in scored if s["suspicion_score"] >= sigma_threshold]

    return {
        "parameters": {
            "sigma_threshold": sigma_threshold,
            "alpha": alpha,
            "beta": beta,
            "min_group_size": min_group_size,
            "robust_stats": use_mad,
        },
        "global_distribution": {
            "center": round(global_center, 4),
            "spread": round(global_spread, 4),
            "method": "median/MAD" if use_mad else "mean/stdev",
        },
        "summary": {
            "total_nodes": len(nodes),
            "connected_nodes": len(connected),
            "disconnected_nodes": len(nodes) - len(connected),
            "total_groups": len(groups),
            "groups_with_sufficient_data": sum(
                1 for gs in group_stats.values() if gs["sufficient"]
            ),
            "groups_fallback_to_global": sum(
                1 for gs in group_stats.values() if not gs["sufficient"]
            ),
            "flagged_count": len(flagged),
        },
        "group_stats": dict(sorted(group_stats.items())),
        "flagged_skills": flagged,
        "all_scored": scored,
    }


def _verdict(global_z: float, group_z: float | None, threshold: float) -> str:
    """Classify based on where the skill lands in global vs group space."""
    g_high = abs(global_z) >= threshold
    if group_z is None:
        return "suspect_global_only" if g_high else "normal"
    grp_high = abs(group_z) >= threshold
    if g_high and grp_high:
        return "likely_false_positive"
    if g_high and not grp_high:
        return "legitimate_hub"
    if not g_high and grp_high:
        return "group_anomaly"
    return "normal"


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="NPL Per-Group Log-Transform Outlier Analysis"
    )
    parser.add_argument(
        "--input", type=Path,
        default=Path(__file__).parent / "Input" / "universe.json",
    )
    parser.add_argument(
        "--output", type=Path,
        default=Path(__file__).parent / "Output" / "group_outliers.json",
    )
    parser.add_argument("--sigma", type=float, default=2.0,
                        help="Z-score / suspicion threshold (default: 2.0)")
    parser.add_argument("--alpha", type=float, default=0.4,
                        help="Weight for global z in suspicion score (default: 0.4)")
    parser.add_argument("--beta", type=float, default=0.6,
                        help="Weight for group z in suspicion score (default: 0.6)")
    parser.add_argument("--min-group-size", type=int, default=5,
                        help="Minimum connected skills for per-group stats (default: 5)")
    parser.add_argument("--no-mad", action="store_true",
                        help="Use mean/stdev instead of median/MAD")
    args = parser.parse_args()

    nodes = load_nodes(args.input)
    results = analyze(
        nodes,
        sigma_threshold=args.sigma,
        alpha=args.alpha,
        beta=args.beta,
        min_group_size=args.min_group_size,
        use_mad=not args.no_mad,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)

    with open(args.output, "w") as f:
        json.dump(results, f, indent=2, sort_keys=False)

    review_path = args.output.with_name("group_review_list.json")
    review = {
        "parameters": results["parameters"],
        "summary": results["summary"],
        "flagged_skills": results["flagged_skills"],
    }
    with open(review_path, "w") as f:
        json.dump(review, f, indent=2, sort_keys=False)

    # ── Console report ───────────────────────────────────────────────
    s = results["summary"]
    g = results["global_distribution"]
    print(f"Connected: {s['connected_nodes']} / {s['total_nodes']} nodes")
    print(f"Global fit ({g['method']}): center={g['center']:.4f}  spread={g['spread']:.4f}")
    print(f"Groups: {s['total_groups']} total, "
          f"{s['groups_with_sufficient_data']} with per-group stats, "
          f"{s['groups_fallback_to_global']} fallback")
    print(f"Flagged: {s['flagged_count']} skills (threshold={args.sigma})")

    flagged = results["flagged_skills"]
    if not flagged:
        print("\nNo skills flagged.")
        return

    for verdict in ["likely_false_positive", "group_anomaly",
                     "suspect_global_only", "legitimate_hub"]:
        subset = [f for f in flagged if f["verdict"] == verdict]
        if not subset:
            continue
        label = verdict.replace("_", " ").upper()
        print(f"\n── {label} ({len(subset)}) ──")
        for sk in subset[:12]:
            gz = f"{sk['global_z']:+.2f}σ"
            grz = f"{sk['group_z']:+.2f}σ" if sk["group_z"] is not None else "  n/a "
            print(f"  {sk['suspicion_score']:5.2f}  g={gz}  grp={grz}  "
                  f"{sk['total_count']:>6}  {sk['skill']:<30} [{sk['group']}]")
        if len(subset) > 12:
            print(f"  ... and {len(subset) - 12} more")

    print(f"\nFull results: {args.output}")
    print(f"Review list:  {review_path}")


if __name__ == "__main__":
    main()
