#!/usr/bin/env python3
"""
NPL Log-Transform Outlier Analysis

Loads universe.json node counts, log-transforms connected nodes,
fits a Gaussian, and flags skills beyond 2σ as outliers.
"""

import argparse
import json
import math
import statistics
from pathlib import Path


def load_nodes(universe_path: Path) -> dict:
    with open(universe_path) as f:
        data = json.load(f)
    return data["nodes"]


def analyze(nodes: dict, sigma_threshold: float = 2.0) -> dict:
    # Filter to connected nodes (count > 0)
    connected = {
        name: info for name, info in nodes.items() if info["total_count"] > 0
    }

    # Log-transform counts
    log_counts = {name: math.log(info["total_count"]) for name, info in connected.items()}

    # Fit Gaussian (mean and stdev of log-counts)
    values = list(log_counts.values())
    mu = statistics.mean(values)
    sigma = statistics.stdev(values)

    # Flag outliers beyond threshold
    flagged = []
    for name, log_c in log_counts.items():
        z_score = (log_c - mu) / sigma
        info = connected[name]
        entry = {
            "skill": name,
            "total_count": info["total_count"],
            "log_count": round(log_c, 4),
            "z_score": round(z_score, 4),
            "group": info["group"],
            "super_group": info["super_group"],
            "direction": "high" if z_score > 0 else "low",
        }
        if abs(z_score) >= sigma_threshold:
            flagged.append(entry)

    # Sort by absolute z-score descending (worst offenders first)
    flagged.sort(key=lambda x: abs(x["z_score"]), reverse=True)

    return {
        "summary": {
            "total_nodes": len(nodes),
            "connected_nodes": len(connected),
            "disconnected_nodes": len(nodes) - len(connected),
            "log_mean": round(mu, 4),
            "log_stdev": round(sigma, 4),
            "sigma_threshold": sigma_threshold,
            "flagged_count": len(flagged),
            "flagged_high": sum(1 for f in flagged if f["direction"] == "high"),
            "flagged_low": sum(1 for f in flagged if f["direction"] == "low"),
        },
        "flagged_skills": flagged,
    }


def main():
    parser = argparse.ArgumentParser(description="NPL Log-Transform Outlier Analysis")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path(__file__).parent / "Input" / "universe.json",
        help="Path to universe.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "Output" / "log_transform_outliers.json",
        help="Path to output JSON",
    )
    parser.add_argument(
        "--sigma",
        type=float,
        default=2.0,
        help="Z-score threshold for flagging (default: 2.0)",
    )
    args = parser.parse_args()

    nodes = load_nodes(args.input)
    results = analyze(nodes, sigma_threshold=args.sigma)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2, sort_keys=False)

    s = results["summary"]
    print(f"Connected nodes: {s['connected_nodes']} / {s['total_nodes']}")
    print(f"Log-normal fit:  μ={s['log_mean']:.4f}  σ={s['log_stdev']:.4f}")
    print(f"Flagged ({s['sigma_threshold']}σ): {s['flagged_count']}  "
          f"(high={s['flagged_high']}, low={s['flagged_low']})")
    print(f"\nTop flagged skills:")
    for skill in results["flagged_skills"][:15]:
        print(f"  {skill['z_score']:+7.2f}σ  {skill['total_count']:>6}  {skill['skill']}")
    print(f"\nResults written to {args.output}")


if __name__ == "__main__":
    main()
