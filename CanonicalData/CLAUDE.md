# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Run the full pipeline (defaults to `Output/output`)
```bash
python3 -m src.pipeline.run
python3 -m src.pipeline.run --input Input/canonical_data.json --out Output/<run_id>
```

### Run with heuristic providers — no external model servers needed
```bash
python3 -m src.pipeline.run --llm-provider heuristic --embedding-provider heuristic --out Output/test
```

### Run a single stage
```bash
python3 -m src.pipeline.run --stage stage0 --out Output/stage0
python3 -m src.pipeline.run --stage stage1 --out Output/stage1
python3 -m src.pipeline.run --stage stage2 --out Output/stage2
python3 -m src.pipeline.run --stage stage3 --out Output/stage3
python3 -m src.pipeline.run --stage stage4 --out Output/stage4
python3 -m src.pipeline.run --stage stage5 --out Output/stage5
python3 -m src.pipeline.run --stage stage6 --out Output/stage6
```

### Resume from prior artifacts (verifies source_hash before resuming)
```bash
python3 -m src.pipeline.run --stage stage3 --out Output/stage3/new-run --resume-from Output/stage2/runA
```

### Use file-backed LLM responses (replay from prior run)
```bash
python3 -m src.pipeline.run \
  --llm-provider file \
  --arbitration-json Output/stage3/stage3_arbitration_decisions.json \
  --classification-json Output/stage4/stage4_classification_decisions.json \
  --out Output/replay
```

### Recompute Stage 4 classification distribution from any run output
```bash
python3 - <<'PY'
import json
from collections import Counter
rows = json.load(open("Output/stage4/stage4_classification_decisions.json"))
onto = Counter(r.get("classification", {}).get("ontological_nature", "") for r in rows)
abst = Counter(r.get("classification", {}).get("abstraction_level", "") for r in rows)
conf = Counter(r.get("confidence", "") for r in rows)
status = Counter(r.get("status", "") for r in rows)
print("ontological:", dict(onto))
print("abstraction:", dict(abst))
print("confidence:", dict(conf))
print("status:", dict(status))
PY
```

### Run tests
```bash
python3 -m pytest tests/
python3 -m unittest tests.test_pipeline
# Run a single test
python3 -m pytest tests/test_pipeline.py::GovernedPipelineTests::test_numpy_numba_alias_blocked
```

## Architecture

### Data model and canonical store

The source-of-truth is `Input/canonical_data.json` with the shape:
```json
{
  "GroupName": {
    "Canonical Term": ["alias 1", "alias 2"]
  }
}
```

**The pipeline never writes back to `Input/canonical_data.json`.** All outputs are advisory artifacts. Human approval is required for any source-of-truth update.

### Stage overview

All stages live in `src/pipeline/stages/<stageName>/stage.py`. Each stage exposes a single `run_stage*` function that accepts plain Python objects and returns a `StageResult`. Stages never share global state — they are called sequentially or concurrently by the runner.

| Stage | Entry point | LLM | Purpose |
|-------|------------|-----|---------|
| 0 | `run_stage0` | No | Deterministic pre-clean: validate schema, detect collisions/atomicity violations, apply safe rewrites |
| 1 | `run_stage1_similarity` | No | Compute pairwise similarity edges (dense HTTP embedding or sparse n-gram heuristic) |
| 2 | `run_stage2_clusters` | No | Convert pairwise edges → conflict clusters via connected components |
| 3 | `run_stage3_arbitration` | Yes | Per-cluster LLM arbitration → deterministic governance override |
| 4 | `run_stage4_classification` | Yes | Per-canonical ontology/abstraction classification → v2 preview records |
| 5 | `run_stage5_graph_validation` | No | Graph-level safety checks over the similarity graph |
| 6 | `run_stage6_diff_reporting` | No | Aggregate all prior outputs into final `proposed_changes.json/md` and `review_queue.jsonl` |

**Important DAG dependency**: Stage 4 does **not** depend on Stage 3. In default DAG mode (`--execution-mode dag`), Stage 3 and Stage 4 run concurrently using `ThreadPoolExecutor` after Stage 2 completes. Stage 4 data is merged into Stage 6 reporting.

### Runner and execution modes (`src/pipeline/runner/pipeline_runner.py`)

The runner is the main orchestrator. `run_pipeline()` dispatches to either `_run_full_pipeline()` (for `--stage all`) or `_run_single_stage()` (for a specific stage).

**DAG mode** (default): Stage 4 is submitted to the thread pool immediately, while Stage 1 → Stage 2 → Stage 3 run on the main thread. Stage 3 and Stage 4 results are joined at the end.

**Serial mode** (`--execution-mode serial`): Runs stages strictly in order 0 → 1 → 2 → 3 → 4 → 5 → 6.

**Resume mode** (`--resume-from <dir>`): The runner detects which stages already have complete artifacts in the prior directory by checking `RESUME_REQUIRED_FILES_BY_STAGE`. It validates the `source_hash` from the prior `stage0_validation_report.json` against the current `--input` hash. On mismatch, execution halts and writes `preflight_error.json`.

**Preflight**: Before any stage runs, the runner verifies that required model servers are reachable. If Stage 1 needs HTTP embeddings, it calls `verify_model_available()` on the embedding client. If Stage 3/4 needs HTTP reasoning, it probes the reasoning endpoint. On failure, it exits with code 3 and writes `preflight_error.json`.

### StageResult and the Finding/governance model (`src/pipeline/shared/models.py`)

Every stage returns a `StageResult` dataclass:
```python
@dataclass
class StageResult:
    findings: List[Finding]    # all governance findings emitted
    blocking_error: bool       # True if any finding has blocking=True
    payload: Dict[str, Any]    # stage output data (similarity_edges, clusters, decisions, etc.)
    parse_error: bool          # True on unrecoverable LLM schema failure
```

`add_finding()` automatically sets `blocking_error = True` if the finding is blocking.

`Finding` carries:
- `rule_id`: `L<layer>-<number>` (see rule legend below)
- `severity`: `"error"` or `"warning"`
- `blocking`: whether this finding gates the pipeline
- `location`: human-readable path like `group:AI.canonical:React`
- `observed_value`, `normalized_value`, `proposed_action`, `proposed_payload`, `reason`

### Client abstraction (`src/pipeline/clients/model_clients.py`)

**LLM provider** (selected by `--llm-provider`):
- `heuristic` → `HeuristicLLMClient`: pure rule-based fallback (slash splits, contextual-prefix detection). Requires no network. Useful for testing.
- `file` → `FileBackedLLMClient`: replays pre-computed JSON decision files. Falls back to heuristic for unknown terms/clusters.
- `http` → `HttpReasoningLLMClient`: posts to a local LLM server (OpenAI-compatible `/v1/chat/completions`, fallback to `/v1/completions`, then `/completion`). Temperature is always 0. If the initial response fails schema validation, it automatically sends a repair prompt (one retry per term/cluster).

**Embedding provider** (selected by `--embedding-provider`):
- `heuristic` (default) → sparse character 3-gram cosine similarity. No network.
- `http` → `HttpEmbeddingClient`: posts to a local embedding server. Tries batch mode via `/v1/embeddings` first, falls back to per-item mode, then legacy `/embedding` endpoint. Vectors are sorted by `index` field in the response.

### Stage 0 — Deterministic Pre-clean (`stage0_deterministic_preclean/stage.py`)

`run_stage0()` flow:
1. **Schema validation** (`validate_schema`): checks the `group → canonical → [aliases]` structure. Fatal errors abort the stage early.
2. **Alias hygiene loop**: for each alias — dedup (L1-009), hard-block check (L1-008 via `is_hard_blocked_alias`), version-token check (L1-007). Blocked/versioned aliases are removed from the rewritten store.
3. **Atomicity check** (`contains_atomicity_violation`): detects `/`, `,`, `( )`, `and`, contextual prefixes (`core `, `advanced `, etc.), and version tokens. If found and not in the exceptions list, emits L1-006 and derives a rewrite decision via `derive_atomicity_rewrite_decision`.
4. **Rewrite application** (`build_rewritten_entries` + `_upsert_rewritten_entries_for_group`): Applies deterministic rewrites (slash splits, parenthetical cleanup, `and` splits). Merges aliases if the split target already exists.
5. **Post-rewrite collision checks**: `_apply_duplicate_canonical_findings` (L1-002), `_apply_alias_multiplicity_findings` (L1-004), `_apply_alias_collision_findings` (L1-003), `_apply_group_collision_findings` (L1-005).
6. **Auto-drop aliases matching canonicals** (`_auto_drop_aliases_matching_existing_canonicals`): emits L1-011, mutates `rewritten_store` in place.
7. **Suffix redundancy audit** (`_collect_suffix_redundancy_candidates`): finds aliases that end with another canonical in the same group. Advisory signal only — no mutations. Passed to Stage 3 review queue.
8. **Output**: populates `result.payload` with `validation_report`, `rewritten_store`, `canonical_rows` (from rewritten store), `original_canonical_rows` (from cleaned store pre-rewrite), `rewrite_plan`, `suffix_redundancy_candidates`.

Rewritten-store validation (`_build_rewritten_store_validation_report`) runs a second pass with `L1R-*` prefixed rule IDs.

Canonicals in `Input/atomicity_exceptions.json` (or the hardcoded defaults: `tcp/ip`, `pl/sql`, etc.) bypass atomicity checks.

**Key rule**: `canonical_rows` consumed by downstream stages always comes from the **rewritten** store, not the original. `original_canonical_rows` is kept for audit purposes only.

### Stage 1 — Embedding similarity (`stage1_embedding_similarity/stage.py`)

`run_stage1_similarity()` takes `canonicals` (flat list of strings from Stage 0 rewritten store) and `canonical_rows` (for alias-to-canonical advisory detection).

- Builds a character n-gram inverted index to find candidate pairs efficiently.
- Computes pairwise similarity: dense cosine (HTTP mode) or sparse 3-gram cosine (heuristic mode).
- Thresholds: `> 0.93` → `high_collision` band; `>= 0.85` → `possible_conflict`; below 0.85 → discarded.
- **Alias-to-canonical advisory**: if an alias of canonical A is itself a canonical B and similarity is high, emits an advisory entry in `alias_canonical_advisories` (passed to Stage 3 review queue).
- `execution` payload records: `input_canonicals`, `embedding_mode`, `dense_embeddings_used`, `embedding_batch_size`.

### Stage 2 — Conflict clustering (`stage2_conflict_clustering/stage.py`)

`run_stage2_clusters()` takes `similarity_edges` and builds undirected weighted adjacency. Uses connected-component finding, then splits oversized clusters deterministically using high-degree seed nodes and weighted-neighbor fill. Default max cluster size: 10.

### Stage 3 — Semantic arbitration (`stage3_semantic_arbitration/stage.py`)

`run_stage3_arbitration()` processes one cluster at a time:
1. Calls `llm_client.arbitrate_cluster(cluster_id, terms)`.
2. Validates response schema (required fields, allowed action set, confidence values).
3. **Governance**: for each decision —
   - `MERGE_AS_ALIAS`/`MARK_AS_CONTEXTUAL`: target must be in the cluster.
   - `SPLIT_INTO_MULTIPLE_CANONICALS`: must include non-empty `split_candidates`.
   - LOW confidence → `effective_action` forced to `KEEP_DISTINCT`, queued for review.
   - Governance violations → `effective_action` forced to `KEEP_DISTINCT`, L3-002 finding.
4. Appends suffix-redundancy candidates and alias-canonical advisories into the review queue at the start (before cluster processing).
5. Checkpoints after every N clusters (`--stage3-checkpoint-every`, default 1). Checkpoint files: `stage3_checkpoint_meta.json`, `*.partial.json`. Resumes from checkpoint automatically on restart.

### Stage 4 — Classification (`stage4_abstraction_classification/stage.py`)

`run_stage4_classification()` processes one canonical at a time:
1. Calls `llm_client.classify_term(canonical)`.
2. Runs `_normalize_classification_response()`: handles malformed LLM output via casefold key-map, infers `ontological_nature`/`abstraction_level` from hints in the response, applies domain overrides (e.g., `computer vision` → `Concept` + `Domain`).
3. Special types: `COMPOSITE_STACK` → L4-004, queued for expansion; `CATEGORY` → L4-005, queued for rejection.
4. Schema validation (`_validate_classification_schema`): `ontological_nature` must be in `ALLOWED_ONTOLOGICAL_NATURE`, `abstraction_level` in `ALLOWED_ABSTRACTION_LEVEL`, `confidence` in `{HIGH, MEDIUM, LOW}`.
5. LOW confidence → `status = "under_review"`, L4-003 (non-blocking), review queue entry.
6. Produces `ClassificationDecision` and `CanonicalRecordV2` records.
7. Checkpoints after every N canonicals (`--stage4-checkpoint-every`, default 1).

**Domain overrides** (`DOMAIN_CANONICAL_OVERRIDES`): a hardcoded set of terms (e.g., `computer vision`, `machine learning`, `deep learning`, `natural language processing`) that always get `ontological_nature = Concept`, `abstraction_level = Domain`, regardless of LLM output.

**HTTP client repair loop**: `HttpReasoningLLMClient.classify_term()` sends a repair prompt if the initial response fails `_is_schema_shaped_classification_response()`. One retry only. If both fail, returns an error dict that triggers L4-001 and sets `parse_error = True`.

### Stage 5 — Graph validation (`stage5_graph_validation/stage.py`)

Takes Stage 1 edges, Stage 2 clusters, Stage 4 classification decisions. Builds adjacency and connected components. Rules:
- **L5-001** (blocking): over-generic node — degree ratio above threshold.
- **L5-002** (warning): phantom node — appears in edges but not in canonicals.
- **L5-003** (warning): embedding/graph/classification disagreement.

### Stage 6 — Diff reporting (`stage6_diff_reporting/stage.py`)

Aggregates all prior outputs into:
- `validation_report.json` — merged findings from Stage 0, 3, 4
- `arbitration_decisions.json` — Stage 3 governed decisions
- `classification_decisions.json` — Stage 4 classification decisions
- `review_queue.jsonl` — merged Stage 3 + Stage 4 review queues, sorted by (stage, term)
- `proposed_changes.json` + `proposed_changes.md` — advisory patch plan with run metadata
- `v2_preview.json` — Stage 4 v2 records

### Shared utilities (`src/pipeline/shared/utilities.py`)

Key functions:
- `normalize_term(value)`: lowercase + strip + collapse whitespace. Used everywhere for canonical comparison.
- `contains_atomicity_violation(value)`: returns list of reason codes (`slash`, `comma`, `parentheses`, `and`, `contextual_prefix`, `version_like`).
- `contains_version_token(value)`: detects version-like tokens (`v1`, `2.0`, `2024`, `64bit`, `es6`, etc.).
- `explicit_split_tokens(term)`: splits on `/`, `,`, `+`, `&`, `and` — used by heuristic LLM client.
- `char_ngrams(term, n=3)`: generates 3-gram set with `_` for spaces.
- `cosine_similarity_sparse(left, right)`: sparse cosine similarity over n-gram vectors.
- `write_json(path, payload)`: always sorts keys for deterministic output (important for artifact diffs).
- `stable_hash_file(path)`: SHA-256 of file contents (used for resume source-hash verification).

### Rule ID legend

Rule IDs follow `L<layer>-<number>`. `L1R-*` rules are emitted by the rewritten-store validation pass inside Stage 0.

| Rule ID | Stage | Blocking | Description |
|---------|-------|----------|-------------|
| L1-001 | 0 | Yes | Schema/type integrity |
| L1-002 | 0 | Yes | Canonical duplicate after normalization |
| L1-003 | 0 | Yes | Alias collides with canonical |
| L1-004 | 0 | Yes | Alias maps to multiple canonicals |
| L1-005 | 0 | Yes | Group name collides with canonical |
| L1-006 | 0 | Yes | Canonical atomicity violation |
| L1-007 | 0 | No | Alias contains version token (removed in rewrite) |
| L1-008 | 0 | Yes | Alias is hard-blocked (not safely interchangeable) |
| L1-009 | 0 | No | Duplicate alias within same canonical |
| L1-010 | 0 | No | No deterministic rewrite available; manual review |
| L1-011 | 0 | No | Alias matches existing canonical (auto-removed) |
| L1-012 | 0 | No | Canonical matches group name (auto-removed) |
| L3-001 | 3 | Yes | Invalid arbitration response schema |
| L3-002 | 3 | Yes | Arbitration decision failed governance |
| L4-001 | 4 | Yes | Invalid classification response envelope |
| L4-002 | 4 | Yes | Classification failed schema validation |
| L4-003 | 4 | No | LOW-confidence classification (queued for review) |
| L4-004 | 4 | No | COMPOSITE_STACK rejected as canonical |
| L4-005 | 4 | No | CATEGORY rejected as canonical |
| L5-001 | 5 | Yes | Over-generic node detection |
| L5-002 | 5 | No | Phantom node detection |
| L5-003 | 5 | No | Embedding/graph/classification disagreement |

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success, no blocking findings |
| 2 | Deterministic governance blocking findings (or resume hash mismatch) |
| 3 | Stage 3/4 invalid LLM output schema (unrecoverable parse error) |
| 4 | Graph safety blocking findings (Stage 5 L5-001) |

### Stage 4 quality baseline

A healthy Stage 4 run over ~2300 canonicals should show:
- `HIGH` confidence ≥ 95%, `LOW` ≤ 1%
- `under_review` count equals `LOW` count
- All findings are `L4-003` (non-blocking)
- `review_queue.jsonl` includes all `LOW` items

Significant drift from this pattern (e.g., too many `Concrete` for known-domain terms) indicates prompt regression or normalization drift and should be investigated before accepting the run.

### Testing patterns

Tests use `GovernedPipelineTests` (unittest). Stages are tested by calling their `run_stage*` functions directly with in-memory fixtures, bypassing the runner. For HTTP clients, tests subclass `HttpReasoningLLMClient` and override `_chat_and_parse_json`. For Stage 3/4, tests use `MockStage3LLM` / `MockStage4LLM` that return pre-canned responses. Integration tests use `tempfile.TemporaryDirectory` and call `run_pipeline(Namespace(...))`.

When testing `run_pipeline` with a minimal `Namespace`, the following attributes are required by the runner and must be present: `input`, `out`, `resume_from`, `exceptions`, `llm_provider`, `arbitration_json`, `classification_json`, `embedding_provider`, `embedding_endpoint`, `reasoning_endpoint`, `embedding_batch_size`, `http_timeout_seconds`, `stage3_checkpoint_every`, `stage4_checkpoint_every`, `embedding_model`, `reasoning_model`, `execution_mode`, `max_workers`, `stage`.
