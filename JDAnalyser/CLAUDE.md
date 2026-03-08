# JDAnalyser — Job Description Discovery Pipeline

## What This Is
A helper service for Career Navigator's DataFactory. Processes raw JD scrapes
(JSONL from Builtin, etc.), discovers new skills not yet in the taxonomy,
deduplicates them, assigns groups, and produces candidates for taxonomy promotion.

## This Project's Scope
✅ Processing raw JD JSONL files through the discovery pipeline
✅ Deduplicating discovered skills (exact, SBERT-based, semantic)
✅ Auditing and reviewing discovered items
✅ Assigning skill groups via LLM agents
✅ Promoting validated discoveries as taxonomy candidates

❌ NOT building the skill graph (that's DataFactory)
❌ NOT cleaning the taxonomy (that's TaxonomyCleaner)
❌ NOT anomaly detection on the graph (that's NLPAnalysis)

## Pipeline Flow
```
input/Builtin/*.jsonl          ← raw scraped JD data
    ↓
discovery/processor.py         — parse JSONL, extract skill candidates
    ↓
discovery/dedup.py             — exact/fuzzy dedup
agents/sbert_dedup.py          — SBERT embedding-based dedup
agents/semantic_dedup.py       — LLM-based semantic dedup
    ↓
discovery/auditor.py           — validate and audit discoveries
    ↓
agents/group_assigner.py       — assign skill group/category (LLM agent)
    ↓
discovery/promoter.py          — promote to taxonomy candidates
    ↓
data/discovery/discovery_queue.json   ← output: skills pending review
data/agents/group_assignments.json    ← output: group assignment results
```

Entry point: `main.py`
Config: `config/settings.yaml`

## Relationship to DataFactory (Main Project)

### Shared Data
- **Taxonomy (READ ONLY):** `input/Taxonomy/canonical_data.json`
  This is a SYMLINK → `/mnt/workspace/CareerNavigator/DataFactory/data/input/taxonomy/canonical_data.json`
  NEVER break this symlink. NEVER edit this file from JDAnalyser.
  All taxonomy changes go through TaxonomyCleaner.

- **JD Input:** Raw JSONL files in `input/Builtin/` come from the scraper
  (builtin_scraper_agent.py in DataFactory/utils). The JSONL schema must
  stay compatible with both DataFactory's reader.py and our processor.py.

### What Flows Back to DataFactory
- discovery_queue.json contains new skill candidates
- group_assignments.json maps discovered skills to groups
- These feed into DataFactory's taxonomy update cycle (via TaxonomyCleaner)

### Discovery → TaxonomyCleaner → DataFactory cycle:
```
JDAnalyser discovers "Kubernetes Operator SDK"
    ↓ (discovery_queue.json)
Human review → approved
    ↓
TaxonomyCleaner adds to canonical_data.json
    ↓ (symlink updates automatically)
DataFactory & JDAnalyser see the new canonical entry
```

## Key Files
- `discovery/processor.py` — main JSONL processing logic
- `discovery/dedup.py` — deduplication orchestration
- `discovery/auditor.py` — validates discovery quality
- `discovery/promoter.py` — promotes discoveries for taxonomy inclusion
- `discovery/taxonomy.py` — reads taxonomy for lookups during discovery
- `agents/group_assigner.py` — LLM-based group assignment
- `agents/sbert_dedup.py` — SBERT similarity dedup
- `agents/semantic_dedup.py` — semantic dedup with checkpointing

## Data Directory
```
input/
├── Builtin/             ← raw JD JSONL files (timestamped)
└── Taxonomy/
    └── canonical_data.json → (symlink to DataFactory)

data/
├── discovery/
│   ├── discovery_queue.json    ← pending skill discoveries
│   └── statuses/               ← per-item status tracking
└── agents/
    ├── group_assignments.json  ← LLM group assignment results
    ├── sbert_dedup.json        ← SBERT dedup results
    └── semantic_dedup.checkpoint.json ← resumable dedup state
```

## Conventions
- Python 3.12, config via settings.yaml
- Tests in tests/ — run with pytest
- Agent checkpointing: semantic_dedup saves progress for resumability
- JSONL files are timestamped: `builtin_structured_jobs_YYYY-MM-DD_HHMMSS.jsonl`
