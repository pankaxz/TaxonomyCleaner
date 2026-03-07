# CLAUDE.md — JDAnalyser

## What This Project Does

JDAnalyser discovers novel skills from DataCrawler JSONL output, deduplicates them against the canonical taxonomy, and promotes approved skills through human review into `CanonicalDataCleaner/Input/canonical_data.json`.

Pipeline position: **DataCrawler → JDAnalyser → CanonicalDataCleaner → DataFactory**

## Commands

```bash
# Run from JDAnalyser/ directory

# Scan crawler JSONL and update discovery queue
python main.py --discover /path/to/builtin_structured_jobs.jsonl

# Generate review file for promotion-ready skills
python main.py --review

# Apply approved promotions to canonical_data.json
python main.py --apply-review

# Tests
pytest tests/

# Lint & format
ruff check discovery/ config/ tests/
ruff format discovery/ config/ tests/
```

## Project Structure

```
main.py                     # CLI entry point (--discover | --review | --apply-review)
config/
  __init__.py               # Config singleton: cfg.get("dotted.key"), cfg.get_abs_path("key")
  settings.yaml             # Paths, thresholds, logging level
discovery/
  processor.py              # DiscoveryProcessor: scan JSONL, extract candidates, update queue
  dedup.py                  # SkillDeduplicator: 4-tier matching against taxonomy
  promoter.py               # PromotionManager: generate review file, apply approvals
  taxonomy.py               # TaxonomyReader: read-only canonical_data.json access
tests/
  test_discovery.py         # Full test suite (dedup, processor, promoter)
data/discovery/
  discovery_queue.json      # Persistent queue (gitignored)
  review_candidates.json    # Generated review file (gitignored)
```

## Key Design Patterns

### 4-Tier Deduplication (`dedup.py`)

Candidates are matched against the taxonomy in priority order:
1. **Exact** (1.0) — case-insensitive alias map lookup
2. **Normalized** (0.95) — strip punctuation, collapse whitespace
3. **Fuzzy** (>=0.85) — `difflib.SequenceMatcher` ratio
4. **Containment** (0.80) — canonical is a substring of candidate

### Static Classes with Cache

All core classes (`TaxonomyReader`, `SkillDeduplicator`, `DiscoveryProcessor`) use class-level `_CACHE` dicts for lazy-loaded data. Each has an `invalidate_cache()` / `invalidate()` method. Tests must call these between runs.

### Discovery Queue Lifecycle

`pending` → `ready_for_promotion` (auto at `seen_count >= threshold`) → human review → `promoted` | `rejected`

### Review Actions

- `approve` — add as new canonical skill under `suggested_group`
- `alias_of:<CanonicalName>` — add as alias of existing canonical
- `reject` — mark rejected in queue, skip

## Configuration (`config/settings.yaml`)

| Key | Purpose | Default |
|-----|---------|---------|
| `taxonomy.canonical_data` | Path to canonical_data.json (source of truth for dedup) | absolute path |
| `crawler.default_jsonl` | Default crawler input | absolute path |
| `discovery.queue_path` | Discovery queue persistence | `data/discovery/discovery_queue.json` |
| `discovery.review_output` | Review candidates output | `data/discovery/review_candidates.json` |
| `discovery.promotion_threshold` | Seen count to auto-promote | `5` |
| `discovery.fuzzy_threshold` | SequenceMatcher cutoff | `0.85` |
| `discovery.max_sample_sources` | Max source URLs per queue entry | `10` |

All relative paths resolve from the JDAnalyser project root via `cfg.get_abs_path()`.

## Integration Points

- **Reads** `CanonicalDataCleaner/Input/canonical_data.json` for dedup (configured via `taxonomy.canonical_data`)
- **Writes** promoted skills back to that same file (only after human review via `--apply-review`)
- **Reads** DataCrawler JSONL output (structured job records with `technical_skills` and `extraction_quality.unmapped_skills`)

## Testing

Tests use `pytest` with an `autouse` fixture that:
- Patches `cfg` to use `tmp_path` for all file I/O
- Provides a `SAMPLE_TAXONOMY` with known skills (Python, JavaScript, C++, AWS, Kubernetes, TensorFlow)
- Invalidates all static caches between tests
- Sets `promotion_threshold=3` (lower than production default of 5)

## Code Style

- Python 3.10+ (uses `X | Y` union syntax)
- Ruff rules: `E`, `F`, `I` (errors, pyflakes, isort)
- `known-first-party = ["discovery", "config"]` for isort
- All JSON output uses `indent=2, ensure_ascii=False, sort_keys` for deterministic diffs
- No hardcoded paths — everything comes from `config/settings.yaml` or CLI args