# JDAnalyser

JDAnalyser discovers novel skills from job description (JD) data, classifies them using LLM and embedding models, and produces an approved delta artifact for the canonical taxonomy — all without modifying the source taxonomy directly.

## Why This Exists

Web scrapers extract `technical_skills` from job postings, but the scraper's skill list is noisy:
- Skills get tagged inconsistently ("Fastify" appears under both "NodeJS Frameworks" and "Backend Systems")
- Abbreviations and variants slip through ("K8s", "Postgres", "node")
- Soft skills, business domains, and generic terms get mixed in

JDAnalyser filters this noise through a multi-stage pipeline: deterministic dedup, LLM classification, embedding similarity, and human review.

## Pipeline Position

```
DataCrawler (JSONL) → JDAnalyser → CanonicalDataCleaner → DataFactory
```

## CLI Workflow

```bash
# 1. Scan crawler JSONL and build discovery queue
python main.py --discover

# 2. LLM classifies ready-for-promotion skills (group, reject, ontology)
python main.py --assign-groups

# 3. Embedding similarity catches obvious aliases (fast, no LLM)
python main.py --sbert-dedup

# 4. LLM catches remaining semantic aliases (skips sbert hits)
python main.py --semantic-dedup

# 5. Generate review file merging all agent outputs
python main.py --review

# 6. Apply human-reviewed decisions
python main.py --apply-review
```

Additional commands:

```bash
# Audit: trace every skill back to its source JD and JSONL file
python main.py --audit

# Force single-process mode (writes to separate file for diffing)
python main.py --discover --no-parallel

# Point to a specific file or directory
python main.py --discover /path/to/file.jsonl
python main.py --discover /path/to/directory/
```

## What Each Step Does

### Step 1: `--discover` (Discovery & Dedup)

Reads all `.jsonl` files from `crawler.input_dir` (or a given path). For each JD record:

1. Extracts candidates from `extraction_quality.unmapped_skills` AND `technical_skills`
2. Strips `[Category]` tags (e.g., `"Python [Languages]"` → `"Python"`)
3. Deduplicates each candidate against the canonical taxonomy using 4 tiers:
   - **Exact** (1.0) — case-insensitive alias/canonical lookup
   - **Normalized** (0.95) — strip punctuation, collapse whitespace
   - **Fuzzy** (≥0.85) — `difflib.SequenceMatcher` ratio
   - **Containment** (0.80) — canonical is substring of candidate
4. Novel skills enter the queue as `pending`
5. Once `seen_count >= 5` (configurable), status auto-promotes to `ready_for_promotion`

Uses `ProcessPoolExecutor` for parallel JSONL parsing and taxonomy matching.

**Output:** `data/discovery/discovery_queue.json` + `data/discovery/statuses/*.json`

### Step 2: `--assign-groups` (LLM Classification)

Takes each `ready_for_promotion` skill and sends it to a local LLM (llama-server) for classification:

- **Group assignment** — which taxonomy group does it belong to?
- **Rejection** — is it a soft skill, generic term, or business domain?
- **Ontological nature** — Software Artifact, Concept, Algorithm, Protocol, Standard/Specification, Human Skill
- **Abstraction level** — Domain, Method, Concrete
- **Confidence** — HIGH, MEDIUM, LOW

Supports checkpoint/resume — saves after every batch. If interrupted, re-running picks up where it stopped.

**Output:** `data/agents/group_assignments.json` (split into `existing`, `new_groups`, `rejected`, `failed`)

### Step 3: `--sbert-dedup` (Embedding Similarity)

Computes cosine similarity between novel skills and all taxonomy entries using a local embedding model (nomic-embed-text via llama-server). Catches aliases that string matching misses:

- "node" → Node.js (short name)
- "K8s" → Kubernetes (abbreviation)
- "Postgres" → PostgreSQL (variant)

Fast — no LLM reasoning, just vector math.

**Output:** `data/agents/sbert_dedup.json`

### Step 4: `--semantic-dedup` (LLM Alias Detection)

For skills not already resolved by sbert-dedup, asks the LLM whether each one is semantically equivalent to an existing canonical in its assigned group. More thorough than embeddings but slower (one LLM call per skill).

Supports checkpoint/resume.

**Output:** `data/agents/semantic_dedup.json`

### Step 5: `--review` (Generate Review File)

Merges all agent outputs into a single `review_candidates.json` for human spot-check:

- Group assignment, reasoning, confidence from `--assign-groups`
- Alias suggestions from `--sbert-dedup` and `--semantic-dedup`
- Pre-filled `action` field: `approve`, `reject`, or `alias_of:CanonicalName`

The human reviewer edits the `action` field — most decisions are pre-made by the agents.

**Output:** `data/discovery/review_candidates.json`

### Step 6: `--apply-review` (Apply Decisions)

Reads the reviewed file and writes an approved delta artifact:

- `approve` → new canonical skill under its assigned group
- `alias_of:ExistingSkill` → alias entry under that canonical
- `reject` → marked rejected in queue, excluded from output

**Output:** `data/discovery/approved_canonical_output.json`

## Project Structure

```
main.py                          # CLI entry point
config/
  __init__.py                    # Config singleton: cfg.get("dotted.key")
  settings.yaml                  # Paths, thresholds, model config
discovery/
  processor.py                   # JSONL scanning, candidate extraction, queue management
  dedup.py                       # 4-tier matching against taxonomy
  taxonomy.py                    # Read-only canonical_data.json access
  promoter.py                    # Review generation, apply approvals
  auditor.py                     # Full audit trail: skill → JD → JSONL file
agents/
  group_assigner.py              # LLM group classification agent
  sbert_dedup.py                 # Embedding-based semantic dedup agent
  semantic_dedup.py              # LLM-based semantic dedup agent
  review_classifier.py           # (stub)
tests/
  test_discovery.py              # Pytest suite
data/
  discovery/                     # Queue, review files, audit reports (gitignored)
  agents/                        # Agent outputs (gitignored)
input/
  Builtin/                       # Crawler JSONL files
```

## Configuration

All settings live in `config/settings.yaml`. Key entries:

| Key | Purpose | Default |
|-----|---------|---------|
| `taxonomy.canonical_data` | Path to canonical_data.json (source of truth) | absolute path |
| `crawler.input_dir` | Default JSONL input directory | `input/Builtin` |
| `discovery.promotion_threshold` | JD count to auto-promote | `5` |
| `discovery.fuzzy_threshold` | SequenceMatcher cutoff | `0.85` |
| `llm.base_url` | Local LLM server URL | `http://localhost:8080/v1` |
| `llm.model` | LLM model identifier | `Qwen3.5-35B-A3B-Q5_K_S.gguf` |
| `llm.batch_size` | Skills per LLM call | `1` |
| `embedding.base_url` | Local embedding server URL | `http://127.0.0.1:8090` |
| `embedding.threshold` | Cosine similarity cutoff for alias | `0.85` |

## External Dependencies

- **LLM server** — llama-server running Qwen3.5-35B-A3B at `localhost:8080` (for `--assign-groups` and `--semantic-dedup`)
- **Embedding server** — llama-server running nomic-embed-text-v1.5 at `127.0.0.1:8090` (for `--sbert-dedup`)
- **Canonical taxonomy** — `canonical_data.json` from DataFactory (read-only, never mutated)
- **Python 3.10+** — uses `X | Y` union syntax
- **No pip dependencies beyond stdlib** — all HTTP calls use `urllib.request`

## Run Tests

```bash
pytest tests/
```

Tests patch config to temp paths and validate matching tiers, queue accumulation, status files, review actions, and the full discover → review → apply flow.