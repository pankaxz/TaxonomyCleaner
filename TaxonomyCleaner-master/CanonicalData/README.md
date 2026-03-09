# Governed Skill Graph Pipeline

A 7-stage pipeline that validates, classifies, and merges skill taxonomy candidates into `canonical_data.json`.

```
Input/canonical_data.json  (or NewCandidates/candidates_clean.json)
         ↓
Stage 0  Deterministic pre-clean & atomicity checks
Stage 1  Embedding similarity edge detection
Stage 2  Conflict cluster formation
Stage 3  Semantic arbitration (LLM)
Stage 4  Abstraction + ontology classification (LLM)
Stage 5  Graph-aware validation
Stage 6  Diff reporting & review artifacts
Stage 7  Merge validated candidates into canonical_data.json
         ↓
Input/canonical_data_merged.json
```

---

## Quick start

Full pipeline (no LLM):

```bash
python3 -m src.pipeline.run \
    --llm-provider heuristic \
    --embedding-provider heuristic \
    --out Output/<run_id>
```

Full pipeline (HTTP models):

```bash
python3 -m src.pipeline.run \
    --input Input/canonical_data.json \
    --out Output/<run_id> \
    --llm-provider http \
    --embedding-provider http
```

---

## Stage-by-stage CLI

All commands assume `cd CanonicalData`. Replace `<OUT>` with your output directory and `<INPUT>` with your input file.

### Stage 0 — Deterministic pre-clean

Fresh run:
```bash
python3 -m src.pipeline.run --stage stage0 \
    --input <INPUT> \
    --out <OUT>
```

No resume — Stage 0 always runs from scratch.

---

### Stage 1 — Embedding similarity

Fresh run:
```bash
python3 -m src.pipeline.run --stage stage1 \
    --input <INPUT> \
    --out <OUT> \
    --embedding-provider http \
    --embedding-endpoint http://127.0.0.1:8090
```

Resume from Stage 0 artifacts:
```bash
python3 -m src.pipeline.run --stage stage1 \
    --input <INPUT> \
    --out <OUT> \
    --resume-from <PRIOR_OUT> \
    --embedding-provider http \
    --embedding-endpoint http://127.0.0.1:8090
```

---

### Stage 2 — Conflict clustering

Fresh run:
```bash
python3 -m src.pipeline.run --stage stage2 \
    --input <INPUT> \
    --out <OUT>
```

Resume from Stage 1 artifacts:
```bash
python3 -m src.pipeline.run --stage stage2 \
    --input <INPUT> \
    --out <OUT> \
    --resume-from <PRIOR_OUT>
```

---

### Stage 3 — Semantic arbitration (LLM)

Fresh run:
```bash
python3 -m src.pipeline.run --stage stage3 \
    --input <INPUT> \
    --out <OUT> \
    --llm-provider http \
    --reasoning-endpoint http://localhost:8001
```

Resume from Stage 2 artifacts (skips completed clusters via checkpoint):
```bash
python3 -m src.pipeline.run --stage stage3 \
    --input <INPUT> \
    --out <OUT> \
    --resume-from <PRIOR_OUT> \
    --llm-provider http \
    --reasoning-endpoint http://localhost:8001
```

---

### Stage 4 — Abstraction + ontology classification (LLM)

Fresh run:
```bash
python3 -m src.pipeline.run --stage stage4 \
    --input <INPUT> \
    --out <OUT> \
    --llm-provider http \
    --reasoning-endpoint http://localhost:8001
```

Resume from Stage 2 artifacts (skips completed canonicals via checkpoint):
```bash
python3 -m src.pipeline.run --stage stage4 \
    --input <INPUT> \
    --out <OUT> \
    --resume-from <PRIOR_OUT> \
    --llm-provider http \
    --reasoning-endpoint http://localhost:8001
```

Note: Stage 4 runs independently of Stage 3 — it consumes Stage 0 canonical rows directly.

---

### Stage 5 — Graph-aware validation

Fresh run:
```bash
python3 -m src.pipeline.run --stage stage5 \
    --input <INPUT> \
    --out <OUT>
```

Resume from Stage 4 artifacts (no LLM needed):
```bash
python3 -m src.pipeline.run --stage stage5 \
    --input <INPUT> \
    --out <OUT> \
    --resume-from <PRIOR_OUT>
```

---

### Stage 6 — Diff reporting

Fresh run:
```bash
python3 -m src.pipeline.run --stage stage6 \
    --input <INPUT> \
    --out <OUT>
```

Resume from Stage 5 artifacts:
```bash
python3 -m src.pipeline.run --stage stage6 \
    --input <INPUT> \
    --out <OUT> \
    --resume-from <PRIOR_OUT>
```

Key outputs: `proposed_changes.json`, `proposed_changes.md`, `v2_preview.json`.

---

### Stage 7 — Merge validated candidates into canonical_data.json

Dry run (shows overlaps and exclusions, writes nothing):
```bash
python3 scripts/stage7_merge_validated.py \
    --v2-preview <OUT>/v2_preview.json \
    --findings <OUT>/stage4_findings.json \
    --existing Input/canonical_data.json \
    --dry-run
```

Full merge:
```bash
python3 scripts/stage7_merge_validated.py \
    --v2-preview <OUT>/v2_preview.json \
    --findings <OUT>/stage4_findings.json \
    --existing Input/canonical_data.json \
    --output Input/canonical_data_merged.json \
    --report <OUT>/stage7_merge_report.json
```

Exclusion rules applied automatically:
- Entries with **blocking findings** (e.g. L4-001 classification timeout) are skipped
- Entries with **LOW confidence** classification are skipped
- Entries **already in canonical_data.json** (as canonical or alias) have their new aliases merged in, not duplicated

---

## Typical full run for new candidates

```bash
INPUT=Input/NewCandidates/candidates_clean.json
OUT=Output/candidate_validation

# Stage 0
python3 -m src.pipeline.run --stage stage0 --input $INPUT --out $OUT

# Stage 1
python3 -m src.pipeline.run --stage stage1 --input $INPUT --out $OUT \
    --resume-from $OUT --embedding-provider http

# Stage 2
python3 -m src.pipeline.run --stage stage2 --input $INPUT --out $OUT \
    --resume-from $OUT

# Stage 4 (skip Stage 3 for candidates — no conflict clusters needed)
python3 -m src.pipeline.run --stage stage4 --input $INPUT --out $OUT \
    --resume-from $OUT --llm-provider http --reasoning-endpoint http://localhost:8001

# Stage 5
python3 -m src.pipeline.run --stage stage5 --input $INPUT --out $OUT \
    --resume-from $OUT

# Stage 6
python3 -m src.pipeline.run --stage stage6 --input $INPUT --out $OUT \
    --resume-from $OUT

# Stage 7 — dry run first, then merge
python3 scripts/stage7_merge_validated.py \
    --v2-preview $OUT/v2_preview.json \
    --findings $OUT/stage4_findings.json \
    --existing Input/canonical_data.json \
    --dry-run

python3 scripts/stage7_merge_validated.py \
    --v2-preview $OUT/v2_preview.json \
    --findings $OUT/stage4_findings.json \
    --existing Input/canonical_data.json \
    --output Input/canonical_data_merged.json \
    --report $OUT/stage7_merge_report.json
```

---

## All CLI options

```
--input                  Path to input JSON (default: Input/canonical_data.json)
--out                    Output directory for artifacts
--resume-from            Resume from prior artifact directory (verifies source hash)
--exceptions             Atomicity exceptions file (default: Input/atomicity_exceptions.json)
--stage                  all|stage0|stage1|stage2|stage3|stage4|stage5|stage6
--llm-provider           heuristic|file|http
--embedding-provider     heuristic|http
--arbitration-json       File-backed arbitration responses (--llm-provider file)
--classification-json    File-backed classification responses (--llm-provider file)
--embedding-endpoint     Embedding server URL (default: http://127.0.0.1:8090)
--reasoning-endpoint     LLM server URL (default: http://localhost:8001)
--embedding-model        Model name (default: nomic-embed-text-v1.5.f16.gguf)
--reasoning-model        Model name (default: qwen3.5-35b)
--embedding-batch-size   Batch size for embeddings (default: 64)
--http-timeout-seconds   HTTP timeout for model calls (default: 120.0)
--stage3-checkpoint-every  Save checkpoint after N clusters (default: 1, 0 = disabled)
--stage4-checkpoint-every  Save checkpoint after N canonicals (default: 1, 0 = disabled)
--execution-mode         dag|serial (default: dag)
--max-workers            Parallel workers for DAG mode (default: 2)
```

---

## Resume behavior

- `--resume-from` points to a prior artifacts directory
- Runner detects highest completed stage from required artifact files
- Runner verifies `source_hash` in `stage0_validation_report.json` matches current `--input`
- On hash mismatch, run stops and writes `preflight_error.json`
- Completed stages are skipped; only missing downstream stages are executed

Required files per stage for resume:

| Stage | Required files |
|-------|---------------|
| 0 | `stage0_validation_report.json`, `stage0_rewritten_store.json` |
| 1 | `stage1_similarity_edges.json`, `stage1_thresholds.json` |
| 2 | `stage2_conflict_clusters.json` |
| 3 | `stage3_arbitration_decisions.json`, `stage3_review_queue.json`, `stage3_findings.json` |
| 4 | `stage4_classification_decisions.json`, `stage4_review_queue.json`, `stage4_findings.json` |
| 5 | `stage5_graph_findings.json`, `stage5_graph_components.json` |

---

## Stage outputs

| Stage | Key outputs |
|-------|-------------|
| 0 | `stage0_validation_report.json`, `stage0_canonical_rows.json`, `stage0_rewritten_store.json`, `stage0_rewrite_plan.json`, `stage0_rewritten_validation_report.json`, `stage0_suffix_redundancy_candidates.json` |
| 1 | `stage1_similarity_edges.json`, `stage1_thresholds.json`, `stage1_execution.json`, `stage1_alias_canonical_advisories.json` |
| 2 | `stage2_conflict_clusters.json` |
| 3 | `stage3_arbitration_decisions.json`, `stage3_review_queue.json`, `stage3_findings.json` + checkpoint files |
| 4 | `stage4_classification_decisions.json`, `stage4_v2_preview.json`, `stage4_review_queue.json`, `stage4_findings.json`, `stage4_findings_summary.json` + checkpoint files |
| 5 | `stage5_graph_findings.json`, `stage5_graph_components.json` |
| 6 | `proposed_changes.json`, `proposed_changes.md`, `v2_preview.json`, `validation_report.json`, `review_queue.jsonl` |
| 7 | `canonical_data_merged.json` (output), `stage7_merge_report.json` |

---

## Rule legend

Rule IDs follow `L<layer>-<rule_number>`.

| Rule | Description |
|------|-------------|
| `L1-001` | Schema/type integrity |
| `L1-002` | Canonical duplicate after normalization |
| `L1-003` | Alias collides with canonical |
| `L1-004` | Alias maps to multiple canonicals |
| `L1-005` | Group name collides with canonical |
| `L1-006` | Canonical atomicity violation |
| `L1-007` | Alias version-token warning |
| `L1-008` | Alias not safely interchangeable |
| `L1-009` | Duplicate alias in same canonical |
| `L1-010` | No deterministic rewrite; manual review |
| `L3-001` | Invalid arbitration response schema |
| `L3-002` | Arbitration decision failed deterministic governance |
| `L4-001` | Invalid classification response envelope (e.g. timeout) |
| `L4-002` | Classification decision failed schema validation |
| `L4-003` | LOW-confidence classification containment warning |
| `L4-004` | Composite stack rejected as canonical |
| `L4-005` | Category rejected as canonical |
| `L5-001` | Over-generic node (degree ratio > 0.70) — blocking |
| `L5-002` | Phantom node (isolated, pair-locked) — warning |
| `L5-003` | Embedding/graph/classification signals disagree — warning |

---

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | Success, no blocking findings |
| `2` | Deterministic governance blocking findings (Stage 0/preflight) |
| `3` | Stage 3/4 invalid LLM output schema or parse failure |
| `4` | Graph safety blocking findings (Stage 5) |

No source canonical file is ever mutated by stages 0–6. Stage 7 writes to a separate output file by default.

---

## Directory structure

```
src/pipeline/
  run.py                            Entry point
  runner/pipeline_runner.py         Orchestrator (DAG/serial/resume)
  clients/model_clients.py          LLM & embedding client abstraction
  shared/
    models.py                       StageResult, Finding
    findings.py                     Finding factory
    utilities.py                    normalize_term, cosine_similarity, etc.
  stages/
    stage0_deterministic_preclean/
    stage1_embedding_similarity/
    stage2_conflict_clustering/
    stage3_semantic_arbitration/
    stage4_abstraction_classification/
    stage5_graph_validation/
    stage6_diff_reporting/

scripts/
  stage7_merge_validated.py         Merge v2_preview into canonical_data.json
  merge_candidates.py               Merge approved JDAnalyser candidates
  find_redundant_tokens.py
  keyword_duplicate_report.py
  resolve_duplicate_keywords.py
  run_stage0_iterative.py

Input/
  canonical_data.json               Source of truth (never mutated by pipeline)
  atomicity_exceptions.json         Skills exempt from atomicity splitting
  hard_block_alias_pairs.json       Pairs that must never be merged
  NewCandidates/candidates_clean.json  New candidates for validation

Output/
  candidate_validation/             Run artifacts for candidate validation
```