"""JDAnalyser — Discovery Queue Pipeline CLI.

Pipeline: DataCrawler -> JDAnalyser -> CanonicalDataCleaner -> DataFactory

Usage:
    # Auto-discover: picks up all .jsonl files from configured input_dir
    python main.py --discover

    # Or point to a specific file or directory
    python main.py --discover /path/to/builtin_structured_jobs.jsonl
    python main.py --discover /path/to/directory/

    # Generate review file for skills that hit promotion threshold
    python main.py --review

    # After human review, write approved decisions to final approved output
    python main.py --apply-review
"""

import argparse
import logging
import sys

from config import cfg


def setup_logging() -> None:
    level_str = cfg.get("logging.level", "INFO").upper()
    level = getattr(logging, level_str, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s", datefmt="%H:%M:%S"
        )
    )

    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)


def cmd_discover(path: str | None = None, *, parallel: bool = True) -> None:
    from pathlib import Path

    from discovery.processor import DiscoveryProcessor

    # No path given → use the configured input directory (crawler.input_dir)
    if path is None:
        path = cfg.get_abs_path("crawler.input_dir")
        print(f"No path given, using configured input_dir: {path}")

    # Sequential mode writes to a separate file so you can diff the two outputs
    out_path = None
    if not parallel:
        default_queue = cfg.get_abs_path("discovery.queue_path")
        p = Path(default_queue)
        out_path = str(p.with_stem(p.stem + "_no_parallel"))

    queue = DiscoveryProcessor.process_jsonl(path, parallel=parallel, out_path=out_path)

    ready = sum(1 for e in queue.values() if e.get("status") == "ready_for_promotion")
    pending = sum(1 for e in queue.values() if e.get("status") == "pending")
    print(
        f"\nQueue: {len(queue)} total — {ready} ready for promotion, {pending} pending"
    )
    if out_path:
        print(f"Output: {out_path}")


def cmd_review() -> None:
    from discovery.promoter import PromotionManager

    path = PromotionManager.generate_review()
    print(f"\nReview file written to: {path}")
    print(
        "Edit the 'action' field for each entry (approve / reject / alias_of:SkillName)"
    )
    print("Then run: python main.py --apply-review")


def cmd_apply_review() -> None:
    from discovery.promoter import PromotionManager

    counts = PromotionManager.apply_review()
    print(
        f"\nApplied: {counts['approved']} approved, {counts['aliased']} aliased, "
        f"{counts['rejected']} rejected, {counts['skipped']} skipped"
    )
    approved_path = cfg.get_abs_path("discovery.approved_output")
    if approved_path:
        print(f"Approved output: {approved_path}")
    if counts["approved"] or counts["aliased"]:
        print("\nNext: run CanonicalDataCleaner using approved output as input")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="JDAnalyser — Discovery Queue Pipeline"
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--discover",
        nargs="?",
        const=True,
        default=None,
        metavar="PATH",
        help="Scan all .jsonl files from configured input_dir (default), or pass a specific file/directory",
    )
    group.add_argument(
        "--review",
        action="store_true",
        help="Generate review_candidates.json from promotion-ready entries",
    )
    group.add_argument(
        "--apply-review",
        action="store_true",
        help="Apply reviewed decisions and write approved canonical-like output",
    )
    group.add_argument(
        "--audit",
        nargs="?",
        const=True,
        default=None,
        metavar="PATH",
        help="Generate full audit report tracing every skill to its source JD and JSONL file",
    )
    group.add_argument(
        "--assign-groups",
        action="store_true",
        help="Run LLM agent to assign taxonomy groups to ready_for_promotion skills",
    )

    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="Disable multiprocessing — run parsing and matching single-process",
    )

    args = parser.parse_args()
    setup_logging()

    if args.discover is not None:
        # True when --discover used without a path, string when path given
        path = None if args.discover is True else args.discover
        cmd_discover(path, parallel=not args.no_parallel)
    elif args.review:
        cmd_review()
    elif args.apply_review:
        cmd_apply_review()
    elif args.assign_groups:
        cmd_assign_groups()
    elif args.audit is not None:
        path = None if args.audit is True else args.audit
        cmd_audit(path)


def cmd_assign_groups() -> None:
    from agents.group_assigner import GroupAssigner

    report = GroupAssigner.run(source="ready_for_promotion")
    meta = report.get("meta", {})
    print(
        f"\nGroup assignment complete: {meta.get('total_skills', 0)} skills"
    )
    print(
        f"  Existing groups: {meta.get('assigned_to_existing_group', 0)}, "
        f"New groups suggested: {meta.get('suggested_new_group', 0)}, "
        f"Failed: {meta.get('failed', 0)}"
    )
    out_dir = cfg.get_abs_path("agents.output_dir") or "data/agents"
    print(f"  Output: {out_dir}/group_assignments.json")


def cmd_audit(path: str | None = None) -> None:
    from discovery.auditor import DiscoveryAuditor

    if path is None:
        print(f"No path given, using configured input_dir: {cfg.get_abs_path('crawler.input_dir')}")

    report = DiscoveryAuditor.audit(path)
    meta = report.get("meta", {})
    print(
        f"\nAudit complete: {meta.get('total_unique_skills', 0)} unique skills traced "
        f"across {meta.get('total_records_scanned', 0)} records"
    )
    print(
        f"  Taxonomy matches: {meta.get('taxonomy_matched', 0)}, "
        f"Novel: {meta.get('novel', 0)}"
    )


if __name__ == "__main__":
    main()
