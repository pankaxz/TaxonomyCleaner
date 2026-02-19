# Governed Skill Graph Pipeline

Run the governed pipeline:

```bash
python3 -m src.pipeline.run --input Input/canonical_data.json --out Output/<run_id>
```

## Quick start

Run the full pipeline with built-in defaults:

```bash
python3 -m src.pipeline.run
```

Run a single stage:

```bash
python3 -m src.pipeline.run --stage stage0
```

Optional arguments:

- `--input Input/canonical_data.json`
- `--out Output/<run_id>`
- `--resume-from <artifacts_dir>` (resume from prior stage artifacts with source-hash verification)
- `--exceptions Input/atomicity_exceptions.json`
- `--llm-provider heuristic|file|http`
- `--arbitration-json <path>` (used when `--llm-provider file`)
- `--classification-json <path>` (used when `--llm-provider file`)
- `--embedding-provider heuristic|http`
- `--embedding-endpoint http://127.0.0.1:8090` (used when `--embedding-provider http`)
- `--reasoning-endpoint http://localhost:8080` (used when `--llm-provider http`)
- `--embedding-batch-size <int>` (default `64`)
- `--http-timeout-seconds <float>` (default `120.0`)
- `--stage3-checkpoint-every <int>` (default `1`, saves after each processed cluster; `0` disables checkpointing)
- `--embedding-model <name>` (default `nomic-embed-text-v1.5.f16.gguf`)
- `--reasoning-model <name>` (default `DeepSeek-R1-Distill-Qwen-32B-Q3_K_M.gguf`)
- `--execution-mode dag|serial` (default `dag`)
- `--max-workers <int>` (default `2`)
- `--stage all|stage0|stage1|stage2|stage3|stage4|stage5|stage6` (default `all`)

Model preflight behavior:

- For stages that execute Stage 1 with `--embedding-provider http`, the runner verifies `--embedding-model` exists at `--embedding-endpoint` before execution.
- For stages that execute Stage 3/4 with `--llm-provider http`, the runner verifies `--reasoning-model` exists at `--reasoning-endpoint` before execution.
- If a required model is missing or unreachable, the runner stops with exit code `3` and writes `preflight_error.json` in the output directory.
- If `--resume-from` is set, the runner verifies the prior run `stage0_validation_report.json` `source_hash` matches the current `--input`. On mismatch, execution stops and writes `preflight_error.json`.

Resume example:

```bash
python3 -m src.pipeline.run --stage stage3 --out Output/stage3/new-run --resume-from Output/stage2/runA
```

## Exact order in your current runner (run.py)

`--execution-mode serial`

1. `Stage 0: stages/stage0_deterministic_preclean/stage.py`
2. `Stage 1: stages/stage1_embedding_similarity/stage.py (Nomic embeddings)`
3. `Stage 2: stages/stage2_conflict_clustering/stage.py`
4. `Stage 3: stages/stage3_semantic_arbitration/stage.py (DeepSeek reasoning)`
5. `Stage 4: stages/stage4_abstraction_classification/stage.py (DeepSeek reasoning)`
6. `Stage 5: stages/stage5_graph_validation/stage.py`
7. `Stage 6: stages/stage6_diff_reporting/stage.py`

## Detailed stage behavior

This section describes exactly what each stage does in this codebase, what it reads, what it writes, and what can block execution.

### Stage 0: Deterministic pre-clean (`stage0_deterministic_preclean`)

Purpose:
- Validate and normalize the canonical store before any semantic modeling.
- Propose deterministic rewrites and apply only safe deterministic rewrites to the Stage 0 rewritten working store.

Input:
- In-memory JSON from `--input` (default `Input/canonical_data.json`).
- Atomicity exception registry from `--exceptions` (default `Input/atomicity_exceptions.json`).

Core processing:
- Check for hard blocks using LLM at `agents/audit_hard_blocks_agent.py` and add them to `artifacts/audit_results/hard_blocks_latest.json`
- Schema/type validation for expected `group -> canonical -> aliases` shape.
- Normalization and dedupe checks over canonicals and aliases.
- Collision checks:
- alias vs canonical collisions,
- alias mapped to multiple canonicals,
- group name vs canonical collisions.
- Atomicity checks for canonical and alias keywords inside the canonicals recursively. 
- If alias post-atomicity check is found to be a canonical on its own, it is removed from the alias.
- Deterministic rewrite planning (`stage0_rewrite_plan.json`) for safe transformations such as:
- parenthetical cleanup (`remove_parentheses`),
- deterministic slash split (`split_on_slash`),
- deterministic `and` split (`split_on_and`),
- alias removals for deterministic invalid aliases (`removed_aliases` in rewrite plan).
- Rewritten-store validation pass after rewrite application.

LLM usage:
- None.

Outputs:
- `stage0_validation_report.json`
- `stage0_canonical_rows.json`
- `stage0_rewritten_store.json`
- `stage0_rewrite_plan.json`
- `stage0_rewritten_validation_report.json`

Blocking behavior:
- Blocking findings are reported with L1 rule IDs (for example duplicates/collisions).
- Stage 0 still writes artifacts even when findings are blocking.

Mutation semantics:
- Never mutates source-of-truth input file.
- Only produces deterministic rewritten working artifacts.
- Downstream stages consume `stage0_canonical_rows.json`, which is derived from the rewritten store.

### Stage 1: Embedding similarity detection (`stage1_embedding_similarity`)

Purpose:
- Generate semantic proximity candidate edges between canonical terms.

Input:
- Flattened canonical terms derived from Stage 0 canonical rows.
- Embedding client:
- HTTP dense embedding mode when `--embedding-provider http`,
- fallback heuristic sparse similarity mode when not using HTTP embedding.

Core processing:
- Builds candidate pairs using token inverted-index overlap.
- Computes similarity:
- dense cosine similarity when embeddings are available,
- sparse n-gram cosine similarity otherwise.
- Applies thresholds:
- `> 0.93` => `high_collision`,
- `>= 0.85` => `possible_conflict`,
- `< 0.85` => ignored.
- Emits deterministic edge list and execution telemetry.

LLM usage:
- None.

Outputs:
- `stage1_similarity_edges.json`
- `stage1_thresholds.json`
- `stage1_execution.json`

Blocking behavior:
- No Stage 1 specific governance blocking code path by itself.
- Model preflight can fail run before stage execution (exit code `3`) if required embedding model is unavailable in HTTP mode.

Mutation semantics:
- No canonical mutations.
- Produces signal artifacts only.

### Stage 2: Conflict cluster formation (`stage2_conflict_clustering`)

Purpose:
- Convert Stage 1 pairwise edges into conflict clusters for arbitration.

Input:
- `stage1_similarity_edges.json` payload in memory.

Core processing:
- Builds undirected weighted adjacency graph from similarity edges.
- Finds connected components.
- Emits small components directly as clusters.
- Splits oversized components deterministically using high-degree seeds and weighted-neighbor fill.
- Enforces max cluster size target (default 10 in stage implementation).

LLM usage:
- None.

Outputs:
- `stage2_conflict_clusters.json`

Blocking behavior:
- No dedicated Stage 2 blocking rule set.

Mutation semantics:
- No canonical mutations.
- Produces deterministic cluster artifacts only.

### Stage 3: Semantic arbitration (`stage3_semantic_arbitration`)

Purpose:
- Ask reasoning model for semantic actions on each conflict cluster, then apply deterministic governance over model output.

Input:
- Conflict clusters from Stage 2.
- Known canonical normalized set from Stage 0.
- LLM provider (`heuristic`, `file`, or `http`).

Core processing:
- Iterates cluster-by-cluster.
- Calls LLM for arbitration decisions.
- Validates each decision deterministically:
- required fields and allowed action set,
- term must belong to cluster,
- confidence must be `HIGH|MEDIUM|LOW`,
- required target for `MERGE_AS_ALIAS` and `MARK_AS_CONTEXTUAL`,
- split policy checks for `SPLIT_INTO_MULTIPLE_CANONICALS`.
- Enforces governance overrides:
- violations force `effective_action=KEEP_DISTINCT`,
- LOW confidence containment keeps distinct and queues review.
- Writes checkpoints after each processed cluster by default (`--stage3-checkpoint-every=1`, `0` disables).

LLM usage:
- Yes (reasoning model).

Outputs:
- `stage3_arbitration_decisions.json`
- `stage3_review_queue.json`
- `stage3_findings.json`
- Checkpoint files:
- `stage3_checkpoint_meta.json`
- `stage3_arbitration_decisions.partial.json`
- `stage3_review_queue.partial.json`
- `stage3_findings.partial.json`

Blocking behavior:
- Invalid/malformed arbitration rows produce L3 findings and can trigger parse error path.
- Governance violations are emitted as `L3-002` blocking findings.
- Run exit code can be `3` for parse/schema failure.

Mutation semantics:
- No direct canonical mutation.
- Outputs governed decisions only.

### Stage 4: Abstraction + ontology classification (`stage4_abstraction_classification`)

Purpose:
- Classify each canonical into ontology and abstraction fields for v2 preview records.

Input:
- Stage 0 canonical rows.
- LLM provider (`heuristic`, `file`, or `http`).

Important dependency note:
- Stage 4 does not consume Stage 3 decisions.
- Stage 4 runs independently from Stage 3 and is merged later in Stage 6 reporting.

Core processing:
- Calls classifier for each canonical.
- Validates classification schema deterministically.
- Handles special type envelopes:
- `COMPOSITE_STACK` => rejected as canonical, queued for review,
- `CATEGORY` => rejected as canonical, queued for review.
- LOW confidence containment:
- sets `status=under_review`,
- adds review queue entry.
- Emits normalized `classification_decisions` and `v2_records`.

LLM usage:
- Yes (reasoning model).

Outputs:
- `stage4_classification_decisions.json`
- `stage4_v2_preview.json`
- `stage4_review_queue.json`
- `stage4_findings.json`

Blocking behavior:
- Invalid classification envelope/schema emits L4 blocking findings.
- Run exit code can be `3` for parse/schema failure.

Mutation semantics:
- No source canonical mutation.
- Produces v2 preview and review queue artifacts.

### Stage 5: Graph-aware validation (`stage5_graph_validation`)

Purpose:
- Apply deterministic graph safety checks over similarity graph, cluster assignments, and classification signals.

Input:
- Stage 1 similarity edges.
- Stage 2 conflict clusters.
- Stage 4 classification decisions.

Core processing:
- Builds graph adjacency and connected components.
- Runs deterministic graph rules:
- over-generic node detection (`L5-001`, blocking) using degree ratio threshold,
- phantom node detection (`L5-002`, warning),
- embedding/graph/classification disagreement (`L5-003`, warning).
- Emits graph findings and component map.

LLM usage:
- None.

Outputs:
- `stage5_graph_findings.json`
- `stage5_graph_components.json`

Blocking behavior:
- Over-generic detections can block via Stage 5 blocking findings.
- Run exit code can be `4` for graph safety blocking.

Mutation semantics:
- No canonical mutation.
- Produces validation-only artifacts.

### Stage 6: Diff reporting and human-review artifacts (`stage6_diff_reporting`)

Purpose:
- Aggregate stage outputs into final review artifacts for manual approval workflow.

Input:
- Stage 0 validation report and findings.
- Stage 3 governed arbitration decisions/findings/review queue (if executed in that run context).
- Stage 4 classification decisions/findings/review queue (if executed in that run context).
- Stage 5 graph findings (if executed in that run context).

Core processing:
- Merges validation findings into unified `validation_report.json`.
- Builds proposed changes payload (advisory patch plan).
- Renders markdown diff report.
- Emits unified review queue JSONL.

LLM usage:
- None in Stage 6 itself.

Outputs:
- `validation_report.json`
- `arbitration_decisions.json`
- `classification_decisions.json`
- `review_queue.jsonl`
- `proposed_changes.json`
- `proposed_changes.md`
- `v2_preview.json`

Blocking behavior:
- Stage 6 itself is reporting-only.
- Final exit code still reflects prior stage blocking/parse/graph outcomes.

Mutation semantics:
- No source writeback.
- Human approval is required outside pipeline for any real source-of-truth update.

### Execution mode behavior (`dag` vs `serial`)

Serial mode:
- Executes in strict stage order.

DAG mode:
- Stage 1 then Stage 2 are sequential (dependency).
- Stage 3 and Stage 4 can run concurrently once dependencies are satisfied.
- Stage 5 waits for Stage 1, Stage 2, and Stage 4 outputs.
- Stage 6 aggregates prior outputs.

### Resume behavior (`--resume-from`)

Resume contract:
- `--resume-from` points to a prior artifacts directory.
- Runner detects highest completed stage based on required artifact files.
- Runner verifies `source_hash` from prior `stage0_validation_report.json` matches current `--input`.
- On hash mismatch, run stops early and writes `preflight_error.json`.
- When valid, runner reuses prior stage artifacts and executes only missing downstream stages.

## Directory structure

```text
src/pipeline/
  run.py
  runner/
    pipeline_runner.py
  clients/
    model_clients.py
  shared/
    models.py
    findings.py
    utilities.py
  stages/
    stage0_deterministic_preclean/
      rules.py
      stage.py
    stage1_embedding_similarity/
      stage.py
    stage2_conflict_clustering/
      stage.py
    stage3_semantic_arbitration/
      stage.py
    stage4_abstraction_classification/
      stage.py
    stage5_graph_validation/
      stage.py
    stage6_diff_reporting/
      stage.py
```

## Stage-by-stage commands

```bash
python3 -m src.pipeline.run --stage stage0 --out Output/stage0

python3 -m src.pipeline.run --stage stage1 --out Output/stage1
python3 -m src.pipeline.run --stage stage1 --out Output/stage1/run-http-check

python3 -m src.pipeline.run --stage stage2 --out Output/stage2

python3 -m src.pipeline.run --stage stage3 --out Output/stage3
python3 -m src.pipeline.run --stage stage3 --out Output/stage3/check

python3 -m src.pipeline.run --stage stage4 --out Output/stage4
python3 -m src.pipeline.run --stage stage5 --out Output/stage5
python3 -m src.pipeline.run --stage stage6 --out Output/stage6
```

## Stage outputs

- `stage0`: `stage0_validation_report.json`, `stage0_rewritten_validation_report.json`, `stage0_canonical_rows.json`, `stage0_rewritten_store.json`, `stage0_rewrite_plan.json`
- `stage1`: Stage 0 outputs + `stage1_similarity_edges.json`, `stage1_thresholds.json`, `stage1_execution.json`
- `stage2`: Stage 1 outputs + `stage2_conflict_clusters.json`
- `stage3`: Stage 2 outputs + `stage3_arbitration_decisions.json`, `stage3_review_queue.json`, `stage3_findings.json`, checkpoint files
- `stage4`: Stage 2 outputs + `stage4_classification_decisions.json`, `stage4_v2_preview.json`, `stage4_review_queue.json`, `stage4_findings.json`
- `stage5`: Stage 4 outputs + `stage5_graph_findings.json`, `stage5_graph_components.json`
- `stage6`: Stage 5 outputs + final report artifacts

## Rule legend

Rule IDs follow `L<layer>-<rule_number>`.

- `L1-*` deterministic pre-clean integrity rules (Stage 0)
- `L3-*` arbitration schema/governance rules (Stage 3)
- `L4-*` classification schema/governance rules (Stage 4)
- `L5-*` graph validation rules (Stage 5)

Current rule codes:

- `L1-001` schema/type integrity
- `L1-002` canonical duplicate after normalization
- `L1-003` alias collides with canonical
- `L1-004` alias maps to multiple canonicals
- `L1-005` group name collides with canonical
- `L1-006` canonical atomicity violation
- `L1-007` alias version-token warning
- `L1-008` alias not safely interchangeable
- `L1-009` duplicate alias in same canonical
- `L1-010` no deterministic rewrite; manual review
- `L3-001` invalid arbitration response schema
- `L3-002` arbitration decision failed deterministic governance
- `L4-001` invalid classification response envelope
- `L4-002` classification decision failed schema validation
- `L4-003` LOW-confidence classification containment warning
- `L4-004` composite stack rejected as canonical
- `L4-005` category rejected as canonical
- `L5-001` over-generic node detection
- `L5-002` phantom node detection
- `L5-003` embedding/graph/classification disagreement

## Artifacts emitted every full run

- `validation_report.json`
- `stage0_validation_report.json`
- `stage0_rewritten_validation_report.json`
- `stage0_canonical_rows.json`
- `stage0_rewritten_store.json`
- `stage0_rewrite_plan.json`
- `arbitration_decisions.json`
- `classification_decisions.json`
- `review_queue.jsonl`
- `proposed_changes.json`
- `proposed_changes.md`
- `stage1_similarity_edges.json`
- `stage1_execution.json`
- `stage2_conflict_clusters.json`
- `stage5_graph_findings.json`
- `v2_preview.json`

## Exit codes

- `0` success with no blocking findings
- `2` deterministic governance blocking findings
- `3` Stage 3/4 invalid LLM output schema
- `4` graph safety blocking findings

No source canonical file is mutated by this pipeline.
