from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import os
import sys
from typing import Any
from typing import Dict
from typing import List
from typing import Set
from typing import Tuple

from ..clients.model_clients import FileBackedLLMClient
from ..clients.model_clients import HeuristicLLMClient
from ..clients.model_clients import HttpEmbeddingClient
from ..clients.model_clients import HttpReasoningLLMClient
from ..shared.models import Finding
from ..shared.models import RunArtifacts
from ..shared.models import StageResult
from ..shared.utilities import load_json_file
from ..shared.utilities import normalize_term
from ..shared.utilities import stable_hash_file
from ..shared.utilities import write_json
from ..shared.utilities import write_jsonl
from ..stages.stage0_deterministic_preclean import run_stage0
from ..stages.stage1_embedding_similarity import run_stage1_similarity
from ..stages.stage2_conflict_clustering import run_stage2_clusters
from ..stages.stage3_semantic_arbitration import run_stage3_arbitration
from ..stages.stage4_abstraction_classification import run_stage4_classification
from ..stages.stage5_graph_validation import run_stage5_graph_validation
from ..stages.stage6_diff_reporting import build_proposed_changes
from ..stages.stage6_diff_reporting import render_markdown_diff

STAGE_CHOICES = [
    "all",
    "stage0",
    "stage1",
    "stage2",
    "stage3",
    "stage4",
    "stage5",
    "stage6",
]


DEFAULT_INPUT_PATH = "Input/canonical_data.json"
DEFAULT_EXCEPTIONS_PATH = "Input/atomicity_exceptions.json"
DEFAULT_OUTPUT_ROOT = "Output"
DEFAULT_EMBEDDING_ENDPOINT = "http://127.0.0.1:8090"
DEFAULT_REASONING_ENDPOINT = "http://localhost:8080"
DEFAULT_EMBEDDING_MODEL = "nomic-embed-text-v1.5.f16.gguf"
DEFAULT_REASONING_MODEL = "DeepSeek-R1-Distill-Qwen-32B-Q3_K_M.gguf"

STAGE_INDEX = {
    "stage0": 0,
    "stage1": 1,
    "stage2": 2,
    "stage3": 3,
    "stage4": 4,
    "stage5": 5,
    "stage6": 6,
    "all": 6,
}

RESUME_REQUIRED_FILES_BY_STAGE = {
    0: [
        "stage0_validation_report.json",
        "stage0_rewritten_store.json",
    ],
    1: [
        "stage1_similarity_edges.json",
        "stage1_thresholds.json",
    ],
    2: [
        "stage2_conflict_clusters.json",
    ],
    3: [
        "stage3_arbitration_decisions.json",
        "stage3_review_queue.json",
        "stage3_findings.json",
    ],
    4: [
        "stage4_classification_decisions.json",
        "stage4_v2_preview.json",
        "stage4_review_queue.json",
        "stage4_findings.json",
    ],
    5: [
        "stage5_graph_findings.json",
        "stage5_graph_components.json",
    ],
}

EXECUTION_PLAN_BY_STAGE = {
    "stage0": [0],
    "stage1": [0, 1],
    "stage2": [0, 1, 2],
    "stage3": [0, 1, 2, 3],
    "stage4": [0, 1, 2, 4],
    "stage5": [0, 1, 2, 4, 5],
    "stage6": [0, 1, 2, 3, 4, 5, 6],
    "all": [0, 1, 2, 3, 4, 5, 6],
}


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Governed skill graph pipeline runner")

    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_PATH,
        help="Path to canonical_data.json input (default: Input/canonical_data.json)",
    )
    parser.add_argument(
        "--out",
        default="",
        help="Output directory for run artifacts. Defaults to Output/output or Output/<stage>.",
    )
    parser.add_argument(
        "--resume-from",
        default="",
        help="Resume from an existing artifacts directory (for example Output/stage2/runA).",
    )
    parser.add_argument(
        "--exceptions",
        default=DEFAULT_EXCEPTIONS_PATH,
        help="Path to atomicity exceptions JSON file",
    )

    parser.add_argument(
        "--llm-provider",
        choices=["heuristic", "file", "http"],
        default="http",
        help="Provider mode for stage 3/4 reasoning outputs",
    )
    parser.add_argument(
        "--arbitration-json",
        default="",
        help="Optional file-backed arbitration decisions JSON",
    )
    parser.add_argument(
        "--classification-json",
        default="",
        help="Optional file-backed classification decisions JSON",
    )

    parser.add_argument(
        "--embedding-provider",
        choices=["heuristic", "http"],
        default="http",
        help="Provider mode for Stage 1 embeddings.",
    )
    parser.add_argument(
        "--embedding-endpoint",
        default=DEFAULT_EMBEDDING_ENDPOINT,
        help="Base URL for embedding server when --embedding-provider http.",
    )
    parser.add_argument(
        "--reasoning-endpoint",
        default=DEFAULT_REASONING_ENDPOINT,
        help="Base URL for reasoning server when --llm-provider http.",
    )

    parser.add_argument(
        "--embedding-batch-size",
        type=int,
        default=64,
        help="Batch size for embedding HTTP requests.",
    )
    parser.add_argument(
        "--http-timeout-seconds",
        type=float,
        default=120.0,
        help="Timeout for HTTP model requests.",
    )
    parser.add_argument(
        "--stage3-checkpoint-every",
        type=int,
        default=1,
        help="Persist Stage 3 partial outputs after each processed cluster (0 disables).",
    )
    parser.add_argument(
        "--stage4-checkpoint-every",
        type=int,
        default=1,
        help="Persist Stage 4 partial outputs after each processed canonical (0 disables).",
    )

    parser.add_argument(
        "--embedding-model",
        default=DEFAULT_EMBEDDING_MODEL,
        help="Embedding model metadata tag",
    )
    parser.add_argument(
        "--reasoning-model",
        default=DEFAULT_REASONING_MODEL,
        help="Reasoning model metadata tag",
    )

    parser.add_argument(
        "--execution-mode",
        choices=["dag", "serial"],
        default="dag",
        help="Execution mode for full pipeline runs. 'dag' runs independent stages concurrently.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=2,
        help="Maximum worker threads for concurrent stage execution in dag mode.",
    )

    parser.add_argument(
        "--stage",
        choices=STAGE_CHOICES,
        default="all",
        help="Run a specific stage and write stage-scoped artifacts, or 'all' for full pipeline.",
    )

    parsed = parser.parse_args(argv)
    return parsed


def load_atomicity_exceptions(path: str) -> Set[str]:
    default_exceptions = {
        "tcp/ip",
        "pl/sql",
        "google cloud pub/sub",
        "ibm z/os",
        "ibm z/vse",
        "ibm z/vm",
        "os/390",
    }

    if not path:
        return default_exceptions

    if not os.path.exists(path):
        return default_exceptions

    data = load_json_file(path)
    exceptions: Set[str] = set(default_exceptions)

    rows: List[object] = []
    if isinstance(data, dict):
        maybe_rows = data.get("atomicity_exceptions", [])
        if isinstance(maybe_rows, list):
            rows = maybe_rows

    for row in rows:
        if not isinstance(row, dict):
            continue

        term = row.get("term_normalized")
        if not isinstance(term, str):
            continue
        if not term.strip():
            continue

        exceptions.add(normalize_term(term))

    return exceptions


def build_artifact_paths(out_dir: str) -> RunArtifacts:
    artifacts = RunArtifacts(
        validation_report_json=os.path.join(out_dir, "validation_report.json"),
        arbitration_decisions_json=os.path.join(out_dir, "arbitration_decisions.json"),
        classification_decisions_json=os.path.join(out_dir, "classification_decisions.json"),
        review_queue_jsonl=os.path.join(out_dir, "review_queue.jsonl"),
        proposed_changes_json=os.path.join(out_dir, "proposed_changes.json"),
        proposed_changes_md=os.path.join(out_dir, "proposed_changes.md"),
    )
    return artifacts


def _requested_stage_index(requested_stage: str) -> int:
    index = STAGE_INDEX.get(requested_stage)
    if index is None:
        return 6
    return index


def _detect_resume_stage_index(resume_from: str) -> int:
    if not resume_from:
        return -1
    if not os.path.isdir(resume_from):
        raise RuntimeError(f"Resume directory does not exist: {resume_from}")

    highest_stage = -1
    for stage_index in range(0, 6):
        if _resume_stage_files_exist(resume_from, stage_index):
            highest_stage = stage_index
            continue
        break

    if highest_stage < 0:
        raise RuntimeError(
            f"Resume directory does not contain stage artifacts: {resume_from}"
        )
    return highest_stage


def _resume_stage_files_exist(resume_from: str, stage_index: int) -> bool:
    required_files = RESUME_REQUIRED_FILES_BY_STAGE.get(stage_index, [])
    for filename in required_files:
        file_path = os.path.join(resume_from, filename)
        if not os.path.exists(file_path):
            return False
    return True


def _validate_resume_source_hash(
    resume_from: str,
    current_source_hash: str,
) -> None:
    validation_path = os.path.join(resume_from, "stage0_validation_report.json")
    if not os.path.exists(validation_path):
        raise RuntimeError(
            "Resume directory is missing stage0_validation_report.json required for hash validation."
        )

    report = load_json_file(validation_path)
    if not isinstance(report, dict):
        raise RuntimeError("Resume stage0_validation_report.json is not a JSON object.")

    resume_hash = report.get("source_hash")
    if not isinstance(resume_hash, str) or not resume_hash.strip():
        raise RuntimeError("Resume stage0_validation_report.json is missing source_hash.")

    if resume_hash != current_source_hash:
        raise RuntimeError(
            "Resume source_hash does not match current input file hash. "
            "Refusing to resume from mismatched artifacts."
        )


def _load_resume_context(args: argparse.Namespace, current_source_hash: str) -> Dict[str, object]:
    resume_from = getattr(args, "resume_from", "")
    context: Dict[str, object] = {
        "enabled": False,
        "resume_from": "",
        "available_stage_index": -1,
    }

    if not resume_from:
        return context

    available_stage_index = _detect_resume_stage_index(resume_from)
    _validate_resume_source_hash(resume_from, current_source_hash)

    context["enabled"] = True
    context["resume_from"] = resume_from
    context["available_stage_index"] = available_stage_index
    return context


def _load_resumed_stage0(resume_from: str) -> StageResult:
    stage0 = StageResult()
    stage0.payload["validation_report"] = load_json_file(os.path.join(resume_from, "stage0_validation_report.json"))
    canonical_rows_path = os.path.join(resume_from, "stage0_canonical_rows.json")
    if os.path.exists(canonical_rows_path):
        stage0.payload["canonical_rows"] = load_json_file(canonical_rows_path)
    else:
        stage0.payload["canonical_rows"] = []
    original_rows_path = os.path.join(resume_from, "stage0_original_canonical_rows.json")
    if os.path.exists(original_rows_path):
        stage0.payload["original_canonical_rows"] = load_json_file(original_rows_path)
    else:
        stage0.payload["original_canonical_rows"] = []

    rewritten_store_path = os.path.join(resume_from, "stage0_rewritten_store.json")
    if os.path.exists(rewritten_store_path):
        stage0.payload["rewritten_store"] = load_json_file(rewritten_store_path)
    else:
        stage0.payload["rewritten_store"] = {}

    cleaned_store_path = os.path.join(resume_from, "stage0_cleaned_store.json")
    if os.path.exists(cleaned_store_path):
        stage0.payload["cleaned_store"] = load_json_file(cleaned_store_path)
    else:
        stage0.payload["cleaned_store"] = stage0.payload["rewritten_store"]

    rewrite_plan_path = os.path.join(resume_from, "stage0_rewrite_plan.json")
    if os.path.exists(rewrite_plan_path):
        stage0.payload["rewrite_plan"] = load_json_file(rewrite_plan_path)
    else:
        stage0.payload["rewrite_plan"] = []

    rewritten_validation_path = os.path.join(resume_from, "stage0_rewritten_validation_report.json")
    if os.path.exists(rewritten_validation_path):
        stage0.payload["rewritten_validation_report"] = load_json_file(rewritten_validation_path)
    else:
        stage0.payload["rewritten_validation_report"] = {}

    suffix_candidates_path = os.path.join(resume_from, "stage0_suffix_redundancy_candidates.json")
    if os.path.exists(suffix_candidates_path):
        stage0.payload["suffix_redundancy_candidates"] = load_json_file(suffix_candidates_path)
    else:
        stage0.payload["suffix_redundancy_candidates"] = []

    findings_path = os.path.join(resume_from, "stage0_findings.json")
    if os.path.exists(findings_path):
        finding_rows = load_json_file(findings_path)
        findings = _deserialize_findings(finding_rows)
    else:
        report = stage0.payload.get("validation_report", {})
        if isinstance(report, dict):
            findings = _deserialize_findings(report.get("findings", []))
        else:
            findings = []

    if not stage0.payload["canonical_rows"]:
        rewritten_store = stage0.payload.get("rewritten_store", {})
        stage0.payload["canonical_rows"] = _build_canonical_rows_from_store(rewritten_store)

    stage0.findings = findings
    stage0.blocking_error = _has_blocking_findings(findings)
    return stage0


def _load_resumed_stage1(resume_from: str) -> StageResult:
    stage1 = StageResult()
    stage1.payload["similarity_edges"] = load_json_file(os.path.join(resume_from, "stage1_similarity_edges.json"))
    stage1.payload["thresholds"] = load_json_file(os.path.join(resume_from, "stage1_thresholds.json"))
    advisory_path = os.path.join(resume_from, "stage1_alias_canonical_advisories.json")
    if os.path.exists(advisory_path):
        stage1.payload["alias_canonical_advisories"] = load_json_file(advisory_path)
    else:
        stage1.payload["alias_canonical_advisories"] = []

    execution_path = os.path.join(resume_from, "stage1_execution.json")
    if os.path.exists(execution_path):
        stage1.payload["execution"] = load_json_file(execution_path)
    else:
        stage1.payload["execution"] = {}
    return stage1


def _load_resumed_stage2(resume_from: str) -> StageResult:
    stage2 = StageResult()
    stage2.payload["conflict_clusters"] = load_json_file(os.path.join(resume_from, "stage2_conflict_clusters.json"))
    return stage2


def _load_resumed_stage3(resume_from: str) -> StageResult:
    stage3 = StageResult()
    stage3.payload["governed_arbitration_decisions"] = load_json_file(
        os.path.join(resume_from, "stage3_arbitration_decisions.json")
    )
    stage3.payload["review_queue_entries"] = load_json_file(os.path.join(resume_from, "stage3_review_queue.json"))
    finding_rows = load_json_file(os.path.join(resume_from, "stage3_findings.json"))
    findings = _deserialize_findings(finding_rows)
    stage3.findings = findings
    stage3.blocking_error = _has_blocking_findings(findings)
    stage3.parse_error = _has_rule_id(findings, {"L3-001"})
    return stage3


def _load_resumed_stage4(resume_from: str) -> StageResult:
    stage4 = StageResult()
    stage4.payload["classification_decisions"] = load_json_file(
        os.path.join(resume_from, "stage4_classification_decisions.json")
    )
    stage4.payload["v2_records"] = load_json_file(os.path.join(resume_from, "stage4_v2_preview.json"))
    stage4.payload["review_queue_entries"] = load_json_file(os.path.join(resume_from, "stage4_review_queue.json"))
    finding_rows = load_json_file(os.path.join(resume_from, "stage4_findings.json"))
    findings = _deserialize_findings(finding_rows)
    stage4.findings = findings
    stage4.blocking_error = _has_blocking_findings(findings)
    stage4.parse_error = _has_rule_id(findings, {"L4-001", "L4-002"})
    return stage4


def _load_resumed_stage5(resume_from: str) -> StageResult:
    stage5 = StageResult()
    stage5.payload["graph_findings"] = load_json_file(os.path.join(resume_from, "stage5_graph_findings.json"))
    stage5.payload["graph_components"] = load_json_file(os.path.join(resume_from, "stage5_graph_components.json"))

    findings = _deserialize_findings(stage5.payload["graph_findings"])
    stage5.findings = findings
    stage5.blocking_error = _has_blocking_findings(findings)
    return stage5


def _deserialize_findings(rows: object) -> List[Finding]:
    findings: List[Finding] = []
    if not isinstance(rows, list):
        return findings

    for row in rows:
        if not isinstance(row, dict):
            continue
        finding = Finding(
            rule_id=str(row.get("rule_id", "")),
            severity=str(row.get("severity", "warning")),
            blocking=bool(row.get("blocking", False)),
            location=str(row.get("location", "")),
            observed_value=str(row.get("observed_value", "")),
            normalized_value=str(row.get("normalized_value", "")),
            proposed_action=str(row.get("proposed_action", "")),
            proposed_payload=_safe_dict(row.get("proposed_payload")),
            reason=str(row.get("reason", "")),
        )
        findings.append(finding)
    return findings


def _safe_dict(value: object) -> Dict[str, object]:
    if isinstance(value, dict):
        safe: Dict[str, object] = {}
        for key, item in value.items():
            safe[str(key)] = item
        return safe
    return {}


def _has_blocking_findings(findings: List[Finding]) -> bool:
    for finding in findings:
        if finding.blocking:
            return True
    return False


def _has_rule_id(findings: List[Finding], rule_ids: Set[str]) -> bool:
    for finding in findings:
        if finding.rule_id in rule_ids:
            return True
    return False


def _build_canonical_rows_from_store(store: object) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    if not isinstance(store, dict):
        return rows

    sorted_groups = sorted(store)
    for group in sorted_groups:
        group_value = store.get(group, {})
        if not isinstance(group_value, dict):
            continue

        sorted_canonicals = sorted(group_value)
        for canonical in sorted_canonicals:
            aliases_value = group_value.get(canonical, [])
            aliases: List[str] = []
            if isinstance(aliases_value, list):
                for alias in aliases_value:
                    aliases.append(str(alias))

            rows.append(
                {
                    "group": str(group),
                    "canonical": str(canonical),
                    "canonical_normalized": normalize_term(str(canonical)),
                    "aliases": aliases,
                }
            )

    return rows


def _stages_for_requested_stage(requested_stage: str) -> List[int]:
    stages = EXECUTION_PLAN_BY_STAGE.get(requested_stage)
    if stages is None:
        return list(EXECUTION_PLAN_BY_STAGE["all"])
    return list(stages)


def _stages_to_execute(requested_stage: str, resume_context: Dict[str, object] | None) -> List[int]:
    planned_stages = _stages_for_requested_stage(requested_stage)
    available_stage_index = -1

    if resume_context is not None:
        enabled = bool(resume_context.get("enabled", False))
        if enabled:
            available_stage_index = int(resume_context.get("available_stage_index", -1))

    stages_to_execute: List[int] = []
    for stage_index in planned_stages:
        if stage_index > available_stage_index:
            stages_to_execute.append(stage_index)

    return stages_to_execute


def run_pipeline(args: argparse.Namespace) -> int:
    requested_stage = getattr(args, "stage", "all")

    if not getattr(args, "out", ""):
        if requested_stage == "all":
            args.out = f"{DEFAULT_OUTPUT_ROOT}/output"
        else:
            args.out = f"{DEFAULT_OUTPUT_ROOT}/{requested_stage}"

    source_hash = stable_hash_file(args.input)
    try:
        resume_context = _load_resume_context(args, source_hash)
    except Exception as exc:  # noqa: BLE001
        os.makedirs(args.out, exist_ok=True)
        write_json(
            os.path.join(args.out, "preflight_error.json"),
            {
                "stage": requested_stage,
                "error": str(exc),
                "resume_from": getattr(args, "resume_from", ""),
            },
        )
        return 2

    stages_to_execute = _stages_to_execute(requested_stage, resume_context)
    preflight_error = _preflight_required_models(args, requested_stage, resume_context)
    if preflight_error is not None:
        os.makedirs(args.out, exist_ok=True)
        write_json(
            os.path.join(args.out, "preflight_error.json"),
            {
                "stage": requested_stage,
                "error": str(preflight_error),
                "resume_from": resume_context.get("resume_from", ""),
                "resume_available_stage_index": int(resume_context.get("available_stage_index", -1)),
                "stages_to_execute": stages_to_execute,
                "embedding_provider": getattr(args, "embedding_provider", "heuristic"),
                "embedding_model": getattr(args, "embedding_model", DEFAULT_EMBEDDING_MODEL),
                "embedding_endpoint": getattr(args, "embedding_endpoint", DEFAULT_EMBEDDING_ENDPOINT),
                "llm_provider": getattr(args, "llm_provider", "heuristic"),
                "reasoning_model": getattr(args, "reasoning_model", DEFAULT_REASONING_MODEL),
                "reasoning_endpoint": getattr(args, "reasoning_endpoint", DEFAULT_REASONING_ENDPOINT),
            },
        )
        return 3

    if requested_stage == "all":
        if bool(resume_context.get("enabled", False)):
            return _run_single_stage(
                args,
                "stage6",
                resume_context=resume_context,
                stage6_metadata_stage="all",
                stage6_execution_mode="serial_resume",
                stage6_max_workers=1,
            )
        return _run_full_pipeline(args)

    return _run_single_stage(args, requested_stage, resume_context=resume_context)


def _preflight_required_models(
    args: argparse.Namespace,
    requested_stage: str,
    resume_context: Dict[str, object] | None = None,
) -> Exception | None:
    stages_to_execute = _stages_to_execute(requested_stage, resume_context)
    requires_embedding = 1 in stages_to_execute
    requires_reasoning = 3 in stages_to_execute or 4 in stages_to_execute

    try:
        if requires_embedding:
            embedding_provider = getattr(args, "embedding_provider", "heuristic")
            if embedding_provider == "http":
                embedding_client = _build_embedding_client(args)
                if embedding_client is not None:
                    embedding_model = getattr(args, "embedding_model", DEFAULT_EMBEDDING_MODEL)
                    if hasattr(embedding_client, "verify_model_available"):
                        embedding_client.verify_model_available(embedding_model)

        if requires_reasoning:
            llm_provider = getattr(args, "llm_provider", "heuristic")
            if llm_provider == "http":
                stage3_client, _stage4_client = _build_llm_clients(args)
                if hasattr(stage3_client, "verify_model_available"):
                    stage3_client.verify_model_available()

        return None
    except Exception as exc:  # noqa: BLE001
        return exc


def _run_full_pipeline(args: argparse.Namespace) -> int:
    os.makedirs(args.out, exist_ok=True)
    artifacts = build_artifact_paths(args.out)

    store, source_hash, atomicity_exceptions = _prepare_stage0_inputs(args)
    stage3_llm_client, stage4_llm_client = _build_llm_clients(args)

    stage0 = run_stage0(store, args.input, source_hash, atomicity_exceptions)
    _write_stage0_outputs(args.out, stage0)
    canonical_rows = stage0.payload.get("canonical_rows", [])
    suffix_redundancy_candidates = stage0.payload.get("suffix_redundancy_candidates", [])

    canonicals: List[str] = []
    for row in canonical_rows:
        canonical = row.get("canonical")
        if canonical:
            canonicals.append(str(canonical))

    embedding_client = _build_embedding_client(args)
    embedding_model = getattr(args, "embedding_model", DEFAULT_EMBEDDING_MODEL)
    embedding_batch_size = int(getattr(args, "embedding_batch_size", 64))

    execution_mode = getattr(args, "execution_mode", "dag")
    max_workers = max(2, int(getattr(args, "max_workers", 2)))

    if execution_mode == "serial":
        serial_context = _run_serial_stages_1_to_4(
            args=args,
            canonicals=canonicals,
            canonical_rows=canonical_rows,
            suffix_redundancy_candidates=suffix_redundancy_candidates,
            embedding_client=embedding_client,
            embedding_model=embedding_model,
            embedding_batch_size=embedding_batch_size,
            stage3_llm_client=stage3_llm_client,
            stage4_llm_client=stage4_llm_client,
        )
    else:
        serial_context = _run_dag_stages_1_to_4(
            args=args,
            canonicals=canonicals,
            canonical_rows=canonical_rows,
            suffix_redundancy_candidates=suffix_redundancy_candidates,
            embedding_client=embedding_client,
            embedding_model=embedding_model,
            embedding_batch_size=embedding_batch_size,
            stage3_llm_client=stage3_llm_client,
            stage4_llm_client=stage4_llm_client,
            max_workers=max_workers,
        )

    stage1 = serial_context["stage1"]
    stage2 = serial_context["stage2"]
    stage3 = serial_context["stage3"]
    stage4 = serial_context["stage4"]

    similarity_edges = stage1.payload.get("similarity_edges", [])
    alias_canonical_advisories = stage1.payload.get("alias_canonical_advisories", [])
    conflict_clusters = stage2.payload.get("conflict_clusters", [])
    arbitration_decisions = stage3.payload.get("governed_arbitration_decisions", [])
    classification_decisions = stage4.payload.get("classification_decisions", [])
    v2_records = stage4.payload.get("v2_records", [])

    stage3_review = stage3.payload.get("review_queue_entries", [])
    stage4_review = stage4.payload.get("review_queue_entries", [])
    review_queue = _merge_review_queue(stage3_review, stage4_review)

    stage5 = run_stage5_graph_validation(
        similarity_edges,
        conflict_clusters,
        classification_decisions,
    )
    graph_findings = stage5.payload.get("graph_findings", [])

    validation_report, merged_findings = _merge_validation_report(stage0, stage3=stage3, stage4=stage4)

    proposed_changes = build_proposed_changes(
        validation_report=validation_report,
        arbitration_decisions=arbitration_decisions,
        classification_decisions=classification_decisions,
        graph_findings=graph_findings,
        review_queue=review_queue,
    )
    proposed_changes["run_metadata"] = _build_run_metadata(args, stage="all", execution_mode=execution_mode, max_workers=max_workers)
    proposed_changes["v2_preview_count"] = len(v2_records)

    markdown = render_markdown_diff(validation_report, proposed_changes)

    _write_full_run_artifacts(
        artifacts=artifacts,
        output_dir=args.out,
        validation_report=validation_report,
        arbitration_decisions=arbitration_decisions,
        classification_decisions=classification_decisions,
        review_queue=review_queue,
        proposed_changes=proposed_changes,
        markdown=markdown,
        similarity_edges=similarity_edges,
        alias_canonical_advisories=alias_canonical_advisories,
        conflict_clusters=conflict_clusters,
        graph_findings=graph_findings,
        v2_records=v2_records,
    )

    if stage3.parse_error:
        return 3

    if stage4.parse_error:
        return 3

    if stage5.blocking_error:
        return 4

    for finding in merged_findings:
        if finding.blocking:
            return 2

    return 0


def _run_single_stage(
    args: argparse.Namespace,
    requested_stage: str,
    resume_context: Dict[str, object] | None = None,
    stage6_metadata_stage: str = "stage6",
    stage6_execution_mode: str = "serial",
    stage6_max_workers: int = 1,
) -> int:
    os.makedirs(args.out, exist_ok=True)

    if resume_context is None:
        resume_context = {"enabled": False, "resume_from": "", "available_stage_index": -1}

    resume_enabled = bool(resume_context.get("enabled", False))
    resume_from = str(resume_context.get("resume_from", ""))
    available_stage_index = int(resume_context.get("available_stage_index", -1))

    stage3_llm_client = None
    stage4_llm_client = None

    def get_llm_clients() -> Tuple[Any, Any]:
        nonlocal stage3_llm_client
        nonlocal stage4_llm_client

        if stage3_llm_client is None or stage4_llm_client is None:
            stage3_llm_client, stage4_llm_client = _build_llm_clients(args)
        return stage3_llm_client, stage4_llm_client

    if resume_enabled and available_stage_index >= 0:
        stage0 = _load_resumed_stage0(resume_from)
    else:
        store, source_hash, atomicity_exceptions = _prepare_stage0_inputs(args)
        stage0 = run_stage0(store, args.input, source_hash, atomicity_exceptions)
    _write_stage0_outputs(args.out, stage0)

    canonical_rows = stage0.payload.get("canonical_rows", [])
    suffix_redundancy_candidates = stage0.payload.get("suffix_redundancy_candidates", [])
    canonicals: List[str] = []
    for row in canonical_rows:
        canonical = row.get("canonical")
        if canonical:
            canonicals.append(str(canonical))

    if requested_stage == "stage0":
        return _compute_exit_code(stage0)

    if resume_enabled and available_stage_index >= 1:
        stage1 = _load_resumed_stage1(resume_from)
    else:
        embedding_client = _build_embedding_client(args)
        embedding_model = getattr(args, "embedding_model", DEFAULT_EMBEDDING_MODEL)
        embedding_batch_size = int(getattr(args, "embedding_batch_size", 64))
        stage1 = run_stage1_similarity(
            canonicals,
            canonical_rows=canonical_rows,
            embedding_client=embedding_client,
            embedding_model=embedding_model,
            embedding_batch_size=embedding_batch_size,
        )
    _write_stage1_outputs(args.out, stage1)
    similarity_edges = stage1.payload.get("similarity_edges", [])
    alias_canonical_advisories = stage1.payload.get("alias_canonical_advisories", [])

    if requested_stage == "stage1":
        return _compute_exit_code(stage0)

    if resume_enabled and available_stage_index >= 2:
        stage2 = _load_resumed_stage2(resume_from)
    else:
        stage2 = run_stage2_clusters(similarity_edges)
    _write_stage2_outputs(args.out, stage2)
    conflict_clusters = stage2.payload.get("conflict_clusters", [])

    if requested_stage == "stage2":
        return _compute_exit_code(stage0)

    known_canonicals = _build_known_canonical_set(canonicals)

    stage3 = None
    if requested_stage in {"stage3", "stage6"}:
        if resume_enabled and available_stage_index >= 3:
            stage3 = _load_resumed_stage3(resume_from)
        else:
            stage3_client, _stage4_client = get_llm_clients()
            stage3 = run_stage3_arbitration(
                conflict_clusters,
                stage3_client,
                known_canonicals,
                alias_canonical_advisories=alias_canonical_advisories,
                suffix_audit_candidates=suffix_redundancy_candidates,
                checkpoint_every=max(0, int(getattr(args, "stage3_checkpoint_every", 5))),
                checkpoint_dir=args.out,
            )
        _write_stage3_outputs(args.out, stage3)

        if requested_stage == "stage3":
            return _compute_exit_code(stage0, stage3=stage3)

    stage4 = None
    if requested_stage in {"stage4", "stage5", "stage6"}:
        if resume_enabled and available_stage_index >= 4:
            stage4 = _load_resumed_stage4(resume_from)
        else:
            _stage3_client, stage4_client = get_llm_clients()
            stage4 = run_stage4_classification(
                canonical_rows,
                stage4_client,
                checkpoint_every=max(0, int(getattr(args, "stage4_checkpoint_every", 1))),
                checkpoint_dir=args.out,
            )
        _write_stage4_outputs(args.out, stage4)

        if requested_stage == "stage4":
            return _compute_exit_code(stage0, stage4=stage4)

    stage5 = None
    if requested_stage in {"stage5", "stage6"}:
        if resume_enabled and available_stage_index >= 5:
            stage5 = _load_resumed_stage5(resume_from)
        else:
            classification_decisions: List[Dict[str, object]] = []
            if stage4 is not None:
                classification_decisions = stage4.payload.get("classification_decisions", [])

            stage5 = run_stage5_graph_validation(similarity_edges, conflict_clusters, classification_decisions)
        _write_stage5_outputs(args.out, stage5)

        if requested_stage == "stage5":
            return _compute_exit_code(stage0, stage4=stage4, stage5=stage5)

    if requested_stage != "stage6":
        return _compute_exit_code(stage0, stage3=stage3, stage4=stage4, stage5=stage5)

    return _run_stage6_reporting(
        args,
        stage0,
        stage3,
        stage4,
        stage5,
        metadata_stage=stage6_metadata_stage,
        metadata_execution_mode=stage6_execution_mode,
        metadata_max_workers=stage6_max_workers,
    )


def _run_stage6_reporting(
    args: argparse.Namespace,
    stage0,
    stage3,
    stage4,
    stage5,
    metadata_stage: str = "stage6",
    metadata_execution_mode: str = "serial",
    metadata_max_workers: int = 1,
) -> int:
    artifacts = build_artifact_paths(args.out)

    arbitration_decisions: List[Dict[str, object]] = []
    if stage3 is not None:
        arbitration_decisions = stage3.payload.get("governed_arbitration_decisions", [])

    classification_decisions: List[Dict[str, object]] = []
    if stage4 is not None:
        classification_decisions = stage4.payload.get("classification_decisions", [])

    graph_findings: List[Dict[str, object]] = []
    if stage5 is not None:
        graph_findings = stage5.payload.get("graph_findings", [])

    stage3_review: List[Dict[str, object]] = []
    if stage3 is not None:
        stage3_review = stage3.payload.get("review_queue_entries", [])

    stage4_review: List[Dict[str, object]] = []
    if stage4 is not None:
        stage4_review = stage4.payload.get("review_queue_entries", [])

    review_queue = _merge_review_queue(stage3_review, stage4_review)

    validation_report, _ = _merge_validation_report(stage0, stage3=stage3, stage4=stage4)

    proposed_changes = build_proposed_changes(
        validation_report=validation_report,
        arbitration_decisions=arbitration_decisions,
        classification_decisions=classification_decisions,
        graph_findings=graph_findings,
        review_queue=review_queue,
    )
    proposed_changes["run_metadata"] = _build_run_metadata(
        args,
        stage=metadata_stage,
        execution_mode=metadata_execution_mode,
        max_workers=metadata_max_workers,
    )

    v2_preview_count = 0
    if stage4 is not None:
        v2_preview_count = len(stage4.payload.get("v2_records", []))
    proposed_changes["v2_preview_count"] = v2_preview_count

    markdown = render_markdown_diff(validation_report, proposed_changes)

    write_json(artifacts.validation_report_json, validation_report)
    write_json(artifacts.arbitration_decisions_json, arbitration_decisions)
    write_json(artifacts.classification_decisions_json, classification_decisions)
    write_jsonl(artifacts.review_queue_jsonl, review_queue)
    write_json(artifacts.proposed_changes_json, proposed_changes)

    with open(artifacts.proposed_changes_md, "w", encoding="utf-8") as handle:
        handle.write(markdown)
        handle.write("\n")

    v2_preview: List[Dict[str, object]] = []
    if stage4 is not None:
        v2_preview = stage4.payload.get("v2_records", [])
    write_json(os.path.join(args.out, "v2_preview.json"), v2_preview)

    return _compute_exit_code(stage0, stage3=stage3, stage4=stage4, stage5=stage5)


def _prepare_stage0_inputs(args: argparse.Namespace) -> Tuple[Dict[str, Any], str, Set[str]]:
    source_hash = stable_hash_file(args.input)
    store = load_json_file(args.input)
    atomicity_exceptions = load_atomicity_exceptions(args.exceptions)
    return store, source_hash, atomicity_exceptions


def _build_llm_clients(args: argparse.Namespace) -> Tuple[Any, Any]:
    provider = getattr(args, "llm_provider", "heuristic")

    arbitration_json = getattr(args, "arbitration_json", "")
    classification_json = getattr(args, "classification_json", "")
    reasoning_endpoint = getattr(args, "reasoning_endpoint", DEFAULT_REASONING_ENDPOINT)
    reasoning_model = getattr(args, "reasoning_model", DEFAULT_REASONING_MODEL)
    timeout_seconds = float(getattr(args, "http_timeout_seconds", 120.0))

    if provider == "file":
        stage3_client = FileBackedLLMClient(arbitration_json or None, classification_json or None)
        stage4_client = FileBackedLLMClient(arbitration_json or None, classification_json or None)
        return stage3_client, stage4_client

    if provider == "http":
        stage3_client = HttpReasoningLLMClient(reasoning_endpoint, reasoning_model, timeout_seconds=timeout_seconds)
        stage4_client = HttpReasoningLLMClient(reasoning_endpoint, reasoning_model, timeout_seconds=timeout_seconds)
        return stage3_client, stage4_client

    stage3_client = HeuristicLLMClient()
    stage4_client = HeuristicLLMClient()
    return stage3_client, stage4_client


def _build_embedding_client(args: argparse.Namespace):
    provider = getattr(args, "embedding_provider", "heuristic")
    if provider != "http":
        return None

    endpoint = getattr(args, "embedding_endpoint", DEFAULT_EMBEDDING_ENDPOINT)
    timeout_seconds = float(getattr(args, "http_timeout_seconds", 120.0))
    return HttpEmbeddingClient(endpoint, timeout_seconds=timeout_seconds)


def _build_known_canonical_set(canonicals: List[str]) -> Set[str]:
    known: Set[str] = set()

    for term in canonicals:
        known.add(normalize_term(term))

    return known


def _run_serial_stages_1_to_4(
    args: argparse.Namespace,
    canonicals: List[str],
    canonical_rows: List[Dict[str, object]],
    suffix_redundancy_candidates: List[Dict[str, object]],
    embedding_client,
    embedding_model: str,
    embedding_batch_size: int,
    stage3_llm_client,
    stage4_llm_client,
) -> Dict[str, Any]:
    stage1 = run_stage1_similarity(
        canonicals,
        canonical_rows=canonical_rows,
        embedding_client=embedding_client,
        embedding_model=embedding_model,
        embedding_batch_size=embedding_batch_size,
    )
    similarity_edges = stage1.payload.get("similarity_edges", [])
    alias_canonical_advisories = stage1.payload.get("alias_canonical_advisories", [])

    stage2 = run_stage2_clusters(similarity_edges)
    conflict_clusters = stage2.payload.get("conflict_clusters", [])

    known_canonicals = _build_known_canonical_set(canonicals)
    stage3 = run_stage3_arbitration(
        conflict_clusters,
        stage3_llm_client,
        known_canonicals,
        alias_canonical_advisories=alias_canonical_advisories,
        suffix_audit_candidates=suffix_redundancy_candidates,
        checkpoint_every=max(0, int(getattr(args, "stage3_checkpoint_every", 5))),
        checkpoint_dir=args.out,
    )

    stage4 = run_stage4_classification(
        canonical_rows,
        stage4_llm_client,
        checkpoint_every=max(0, int(getattr(args, "stage4_checkpoint_every", 1))),
        checkpoint_dir=args.out,
    )

    context: Dict[str, Any] = {
        "stage1": stage1,
        "stage2": stage2,
        "stage3": stage3,
        "stage4": stage4,
    }
    return context


def _run_dag_stages_1_to_4(
    args: argparse.Namespace,
    canonicals: List[str],
    canonical_rows: List[Dict[str, object]],
    suffix_redundancy_candidates: List[Dict[str, object]],
    embedding_client,
    embedding_model: str,
    embedding_batch_size: int,
    stage3_llm_client,
    stage4_llm_client,
    max_workers: int,
) -> Dict[str, Any]:
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        stage4_future = executor.submit(
            run_stage4_classification,
            canonical_rows,
            stage4_llm_client,
            max(0, int(getattr(args, "stage4_checkpoint_every", 1))),
            args.out,
        )

        stage1 = run_stage1_similarity(
            canonicals,
            canonical_rows=canonical_rows,
            embedding_client=embedding_client,
            embedding_model=embedding_model,
            embedding_batch_size=embedding_batch_size,
        )
        similarity_edges = stage1.payload.get("similarity_edges", [])
        alias_canonical_advisories = stage1.payload.get("alias_canonical_advisories", [])

        stage2 = run_stage2_clusters(similarity_edges)
        conflict_clusters = stage2.payload.get("conflict_clusters", [])

        known_canonicals = _build_known_canonical_set(canonicals)
        stage3_future = executor.submit(
            run_stage3_arbitration,
            conflict_clusters,
            stage3_llm_client,
            known_canonicals,
            alias_canonical_advisories,
            suffix_redundancy_candidates,
            max(0, int(getattr(args, "stage3_checkpoint_every", 5))),
            args.out,
        )

        stage3 = stage3_future.result()
        stage4 = stage4_future.result()

    context: Dict[str, Any] = {
        "stage1": stage1,
        "stage2": stage2,
        "stage3": stage3,
        "stage4": stage4,
    }
    return context


def _merge_review_queue(
    stage3_review: List[Dict[str, object]],
    stage4_review: List[Dict[str, object]],
) -> List[Dict[str, object]]:
    merged: List[Dict[str, object]] = []

    for row in stage3_review:
        merged.append(row)
    for row in stage4_review:
        merged.append(row)

    merged.sort(
        key=lambda row: (
            int(row.get("stage", 0)),
            str(row.get("term", "")),
        )
    )
    return merged


def _merge_validation_report(stage0, stage3=None, stage4=None):
    findings = []

    for finding in stage0.findings:
        findings.append(finding)

    if stage3 is not None:
        for finding in stage3.findings:
            findings.append(finding)

    if stage4 is not None:
        for finding in stage4.findings:
            findings.append(finding)

    merged = dict(stage0.payload["validation_report"])

    sorted_findings = sorted(
        findings,
        key=lambda item: (
            item.rule_id,
            item.location,
            item.observed_value,
        ),
    )

    serialized_findings: List[Dict[str, object]] = []
    for finding in sorted_findings:
        serialized_findings.append(finding.to_dict())

    merged["findings"] = serialized_findings

    errors = 0
    warnings = 0
    blocking = 0
    for finding in findings:
        if finding.severity == "error":
            errors += 1
        if finding.severity == "warning":
            warnings += 1
        if finding.blocking:
            blocking += 1

    merged["summary"] = {
        "errors": errors,
        "warnings": warnings,
        "blocking": blocking,
    }

    return merged, findings


def _compute_exit_code(stage0, stage3=None, stage4=None, stage5=None) -> int:
    if stage3 is not None and stage3.parse_error:
        return 3

    if stage4 is not None and stage4.parse_error:
        return 3

    if stage5 is not None and stage5.blocking_error:
        return 4

    findings = []
    for finding in stage0.findings:
        findings.append(finding)

    if stage3 is not None:
        for finding in stage3.findings:
            findings.append(finding)

    if stage4 is not None:
        for finding in stage4.findings:
            findings.append(finding)

    for finding in findings:
        if finding.blocking:
            return 2

    return 0


def _build_run_metadata(
    args: argparse.Namespace,
    stage: str,
    execution_mode: str,
    max_workers: int,
) -> Dict[str, object]:
    metadata = {
        "embedding_model": args.embedding_model,
        "reasoning_model": args.reasoning_model,
        "llm_provider": args.llm_provider,
        "embedding_provider": getattr(args, "embedding_provider", "heuristic"),
        "embedding_endpoint": getattr(args, "embedding_endpoint", DEFAULT_EMBEDDING_ENDPOINT),
        "reasoning_endpoint": getattr(args, "reasoning_endpoint", DEFAULT_REASONING_ENDPOINT),
        "execution_mode": execution_mode,
        "max_workers": max_workers,
        "stage": stage,
    }
    return metadata


def _write_full_run_artifacts(
    artifacts: RunArtifacts,
    output_dir: str,
    validation_report: Dict[str, object],
    arbitration_decisions: List[Dict[str, object]],
    classification_decisions: List[Dict[str, object]],
    review_queue: List[Dict[str, object]],
    proposed_changes: Dict[str, object],
    markdown: str,
    similarity_edges: List[Dict[str, object]],
    alias_canonical_advisories: List[Dict[str, object]],
    conflict_clusters: List[Dict[str, object]],
    graph_findings: List[Dict[str, object]],
    v2_records: List[Dict[str, object]],
) -> None:
    write_json(artifacts.validation_report_json, validation_report)
    write_json(artifacts.arbitration_decisions_json, arbitration_decisions)
    write_json(artifacts.classification_decisions_json, classification_decisions)
    write_jsonl(artifacts.review_queue_jsonl, review_queue)
    write_json(artifacts.proposed_changes_json, proposed_changes)

    with open(artifacts.proposed_changes_md, "w", encoding="utf-8") as handle:
        handle.write(markdown)
        handle.write("\n")

    write_json(os.path.join(output_dir, "stage1_similarity_edges.json"), similarity_edges)
    write_json(
        os.path.join(output_dir, "stage1_alias_canonical_advisories.json"),
        alias_canonical_advisories,
    )
    write_json(os.path.join(output_dir, "stage2_conflict_clusters.json"), conflict_clusters)
    write_json(os.path.join(output_dir, "stage5_graph_findings.json"), graph_findings)
    write_json(os.path.join(output_dir, "v2_preview.json"), v2_records)


def _write_stage0_outputs(
    out_dir: str,
    stage0,
) -> None:
    write_json(os.path.join(out_dir, "stage0_validation_report.json"), stage0.payload["validation_report"])
    write_json(
        os.path.join(out_dir, "stage0_rewritten_validation_report.json"),
        stage0.payload.get("rewritten_validation_report", {}),
    )
    write_json(os.path.join(out_dir, "stage0_canonical_rows.json"), stage0.payload.get("canonical_rows", []))
    write_json(os.path.join(out_dir, "stage0_rewritten_store.json"), stage0.payload.get("rewritten_store", {}))
    write_json(os.path.join(out_dir, "stage0_rewrite_plan.json"), stage0.payload.get("rewrite_plan", []))
    write_json(
        os.path.join(out_dir, "stage0_suffix_redundancy_candidates.json"),
        stage0.payload.get("suffix_redundancy_candidates", []),
    )


def _write_stage1_outputs(out_dir: str, stage1) -> None:
    write_json(os.path.join(out_dir, "stage1_similarity_edges.json"), stage1.payload.get("similarity_edges", []))
    write_json(os.path.join(out_dir, "stage1_thresholds.json"), stage1.payload.get("thresholds", {}))
    write_json(
        os.path.join(out_dir, "stage1_alias_canonical_advisories.json"),
        stage1.payload.get("alias_canonical_advisories", []),
    )
    write_json(os.path.join(out_dir, "stage1_execution.json"), stage1.payload.get("execution", {}))


def _write_stage2_outputs(out_dir: str, stage2) -> None:
    write_json(os.path.join(out_dir, "stage2_conflict_clusters.json"), stage2.payload.get("conflict_clusters", []))


def _write_stage3_outputs(out_dir: str, stage3) -> None:
    write_json(
        os.path.join(out_dir, "stage3_arbitration_decisions.json"),
        stage3.payload.get("governed_arbitration_decisions", []),
    )
    write_json(
        os.path.join(out_dir, "stage3_review_queue.json"),
        stage3.payload.get("review_queue_entries", []),
    )

    finding_rows = []
    for finding in stage3.findings:
        finding_rows.append(finding.to_dict())
    write_json(os.path.join(out_dir, "stage3_findings.json"), finding_rows)


def _write_stage4_outputs(out_dir: str, stage4) -> None:
    write_json(
        os.path.join(out_dir, "stage4_classification_decisions.json"),
        stage4.payload.get("classification_decisions", []),
    )
    write_json(os.path.join(out_dir, "stage4_v2_preview.json"), stage4.payload.get("v2_records", []))
    write_json(os.path.join(out_dir, "stage4_review_queue.json"), stage4.payload.get("review_queue_entries", []))

    finding_rows = []
    for finding in stage4.findings:
        finding_rows.append(finding.to_dict())
    write_json(os.path.join(out_dir, "stage4_findings.json"), finding_rows)


def _write_stage5_outputs(out_dir: str, stage5) -> None:
    write_json(os.path.join(out_dir, "stage5_graph_findings.json"), stage5.payload.get("graph_findings", []))
    write_json(os.path.join(out_dir, "stage5_graph_components.json"), stage5.payload.get("graph_components", {}))


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv if argv is not None else sys.argv[1:])
    return run_pipeline(args)
