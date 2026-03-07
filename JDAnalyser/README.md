# JDAnalyser

JDAnalyser discovers candidate skills from crawler JSONL output, removes anything already in the canonical taxonomy, and manages a human-review queue before producing an approved delta artifact.

Pipeline position:

`DataCrawler -> JDAnalyser -> CanonicalDataCleaner -> DataFactory`

## What It Produces

JDAnalyser does **not** edit the source taxonomy file directly. It writes artifacts that can be consumed downstream:

- Discovery queue: `data/discovery/discovery_queue.json`
- Queue split by status: `data/discovery/statuses/*.json`
- Human review file: `data/discovery/review_candidates.json`
- Approved output delta: `data/discovery/approved_canonical_output.json`

All paths are configurable in `config/settings.yaml`.

## CLI Workflow

```bash
# 1) Scan crawler JSONL and update discovery queue
python main.py --discover /path/to/builtin_structured_jobs.jsonl

# Optional: force single-process mode
python main.py --discover /path/to/builtin_structured_jobs.jsonl --no-parallel

# 2) Generate review file for promotion-ready entries
python main.py --review

# 3) Apply reviewed actions and write approved output delta
python main.py --apply-review
```

Notes:

- `--discover --no-parallel` writes to a sibling queue file with `_no_parallel` suffix (for diffing parallel vs sequential output).
- Logging is configured via `logging.level` in `config/settings.yaml`.

## Expected Input Shape

Each JSONL line is a job record. JDAnalyser uses:

- `record.extraction_quality.unmapped_skills` (new crawler signal)
- `record.technical_skills` (can include `[Group Tag]` suffixes)
- `record.source_url` (stored as provenance sample)

Example:

```json
{
  "technical_skills": ["LangGraph [AI Data Science]", "Python [Languages]"],
  "extraction_quality": {"unmapped_skills": ["LangGraph"]},
  "source_url": "https://example.com/job/123"
}
```

## Processing Flow

### 1. Candidate extraction (`discovery/processor.py`)

- Reads JSONL records (parallel parsing when input is large).
- Extracts candidates from:
  - `unmapped_skills`
  - tagged `technical_skills`
- For tagged skills, splits `"Skill [Group_Name]"` into:
  - `name = "Skill"`
  - `group_tag = "Group Name"` (underscores converted to spaces)
- Deduplicates per record and enriches missing tags when later occurrences include a tag.

### 2. Taxonomy dedup (`discovery/dedup.py` + `discovery/taxonomy.py`)

Each unique candidate is checked against `taxonomy.canonical_data` using 4 tiers:

1. Exact canonical/alias match (confidence `1.0`)
2. Normalized punctuation-insensitive match (`0.95`)
3. Fuzzy match via `difflib.SequenceMatcher` (`>= discovery.fuzzy_threshold`)
4. Containment match where canonical is substring of candidate (`0.80`)

Also filters exact group-name matches as `group_exact`.

Anything matched is considered already known and is **not** added to discovery queue.

### 3. Queue update (`DiscoveryProcessor.process_jsonl`)

For novel skills:

- Key: lowercased skill name with spaces replaced by underscores
- Tracks:
  - `seen_count`
  - `first_seen`, `last_seen`
  - `suggested_groups` (tag frequency map)
  - `llm_group_tags` (same tag counts, currently mirrored)
  - `sample_sources` (capped by `discovery.max_sample_sources`)
  - `status`

Status lifecycle:

`pending -> ready_for_promotion -> promoted | rejected`

Transition to `ready_for_promotion` happens automatically when
`seen_count >= discovery.promotion_threshold`.

### 4. Review generation (`discovery/promoter.py::generate_review`)

Selects only `ready_for_promotion` entries and writes review candidates sorted by `seen_count` descending.

Per candidate it pre-fills:

- `action = "alias_of:<Canonical>"` if display name already maps to taxonomy alias/canonical
- `action = "reject"` if display name is a taxonomy group name
- otherwise `action = "approve"`

It also picks `suggested_group` as the most frequent observed group tag.

### 5. Review apply (`discovery/promoter.py::apply_review`)

Reads reviewed actions and:

- `approve`: adds a new canonical in approved output under `suggested_group`
- `alias_of:X`: adds alias under canonical `X` (resolved from current run output or source taxonomy)
- `reject`: marks queue entry rejected
- unknown/invalid actions: counted as skipped

Queue statuses are updated (`promoted` / `rejected`) and saved.
Approved delta is written to `discovery.approved_output`.

## Data Contracts

### Discovery queue entry

```json
{
  "display_name": "LangGraph",
  "seen_count": 4,
  "first_seen": "2026-03-01",
  "last_seen": "2026-03-06",
  "suggested_groups": {"AI Data Science": 4},
  "llm_group_tags": {"AI Data Science": 4},
  "sample_sources": ["https://example.com/job/123"],
  "status": "ready_for_promotion"
}
```

### Review candidate entry

```json
{
  "display_name": "LangGraph",
  "seen_count": 4,
  "suggested_group": "AI Data Science",
  "all_suggested_groups": {"AI Data Science": 4},
  "sample_sources": ["https://example.com/job/123"],
  "action": "approve"
}
```

### Approved output delta

```json
{
  "AI Data Science": {
    "LangGraph": [],
    "TensorFlow": ["TF2"]
  }
}
```

This structure mirrors the canonical taxonomy shape: `{group: {canonical: [aliases]}}`.

## Performance Characteristics

- Multiprocessing is used for:
  - JSONL parsing/candidate extraction
  - dedup matching
- Default worker count: `os.cpu_count()`
- Parallel mode is skipped for small inputs (`< 50` items) to avoid overhead.

## Repository Map

- `main.py`: CLI and command wiring
- `config/__init__.py`: `cfg` loader with dot-key access + absolute path resolution
- `config/settings.yaml`: runtime paths and thresholds
- `discovery/processor.py`: JSONL ingestion, extraction, dedup orchestration, queue persistence
- `discovery/dedup.py`: skill matching logic
- `discovery/taxonomy.py`: cached taxonomy reader/lookup maps
- `discovery/promoter.py`: review generation and reviewed-action application
- `tests/test_discovery.py`: unit + end-to-end pipeline tests

## Run Tests

```bash
pytest tests/
```

Tests patch config to temp paths and validate:

- matching tiers and thresholds
- queue accumulation across runs
- status-file generation
- review action defaults
- full discover -> review -> apply flow
