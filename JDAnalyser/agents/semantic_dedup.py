"""Agent: Semantic Deduplicator

Catches semantic equivalences that fuzzy string matching (SequenceMatcher) misses.
Examples the current 4-tier dedup cannot catch:
  - "K8s Orchestration" vs "Kubernetes"
  - "React.js" vs "React"
  - "ML Ops" vs "MLOps"
  - "Postgres" vs "PostgreSQL"

Input:  novel skill candidates + full taxonomy (canonicals + aliases)
Output: suggested alias mappings with confidence scores and reasoning

Runs after the deterministic dedup pass to catch remaining duplicates.
"""