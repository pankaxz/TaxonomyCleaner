# NLPAnalysis — Skill Graph Quality & Anomaly Detection

## What This Is
A helper service for Career Navigator's DataFactory. Reads the universe.json
skill graph and performs statistical/NLP analysis to find anomalies, outliers,
and group quality issues. Includes React-based visualizations for exploring results.

## This Project's Scope
✅ Statistical anomaly detection on the skill graph
✅ Group outlier identification
✅ Log-transform based outlier detection
✅ Interactive visualizations of graph quality
✅ Producing review lists for manual inspection

❌ NOT building or modifying the graph (that's DataFactory)
❌ NOT processing raw JDs (that's JDAnalyser)
❌ NOT editing the taxonomy (that's TaxonomyCleaner)

## How It Works
```
Input/universe.json                ← copied from DataFactory output
    ↓
main.py / group_analysis.py       — statistical analysis
    ↓
Output/
├── group_outliers.json            ← skills that don't fit their group
├── group_review_list.json         ← groups needing human review
└── log_transform_outliers.json    ← statistically anomalous entries
```

## Relationship to DataFactory (Main Project)
- **Input:** `Input/universe.json` is a copy of
  `DataFactory/data/output/universe.json`
  Copy it fresh before each analysis run.
- **Output feeds back:** Outlier findings inform:
  - GraphAuditor improvements in DataFactory
  - Group reassignment in JDAnalyser (group_assigner.py)
  - Taxonomy cleanup in TaxonomyCleaner

## Visualizations
`Vissualizations/Vissualizations/` is a Vite + React + TypeScript app.
- `skill_anomaly_detection.jsx` — React component for anomaly exploration
- Run: `cd Vissualizations/Vissualizations && npm run dev`
- Reads analysis output JSON files for interactive exploration

## Key Files
- `main.py` — entry point, runs analysis pipeline
- `group_analysis.py` — group-level statistical analysis
- `skill_anomaly_detection.jsx` — visualization component
- `Vissualizations/` — full React app for interactive data exploration

## Conventions
- Python for analysis, React+TypeScript for visualization
- Input/Output directories follow the DataFactory pattern
- Universe.json must match DataFactory's current output schema
