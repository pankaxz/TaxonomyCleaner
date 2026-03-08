# NLP-Analysis: Taxonomy & Skill Anomaly Visualization

A specialized dashboard for analyzing the health, connectivity, and statistical anomalies within large-scale skill taxonomies derived from job descriptions.

## Dashboard Overview

The application is split into two primary analytic views: **Anomaly Detection** and **Taxonomy Health**.

---

### 1. Taxonomy Health Dashboard

This dashboard provides a multi-layered diagnostic view of the skill co-occurrence graph.

#### **Overview Tab**
Vital signs for the current taxonomy state:
*   **Total Skills (2,194):** The full taxonomy size, including skills not yet encountered in JDs.
*   **Connected (57.3%):** Skills that have appeared in at least one JD. This is the "alive" portion of the taxonomy.
*   **Isolated (42.7%):** Skills that contribute nothing to the graph (dead weight for pathfinding).
*   **Edges (102,685):** Total connections. The **Median Weight of 2** is critical—it indicates that half of the edges are backed by only 1-2 JDs, suggesting thin evidence for relationships.
*   **Data Source (3,511 JDs):** Sufficient for core domains (Backend, Cloud, AI), but peripheral for others (Gaming, Blockchain).

**Health Indicators:**
*   **Coverage (57% - Warning):** Ideally 70%+. Below 50% is critical.
*   **Noise (44% - Critical):** 44% of all edges have weight 1 (single coincidental co-occurrences).
*   **False Positives (41 - Critical):** Skills flagged as likely corrupting the graph center of gravity.

#### **Group Balance Tab**
Reveals taxonomy granularity imbalance via super-group distribution.
*   **Granularity:** Backend (37 groups) is extremely fine-grained, while Blockchain (2 groups) is coarse.
*   **Product Impact:** Affects pathfinding depth. Users navigating through Backend have rich intermediate steps; peripheral domains offer minimal routing detail.

#### **Edge Quality Tab** (Diagnostic Core)
*   **Weight Distribution:** Shows that 74.4% of all edges are backed by fewer than 5 JDs. The graph is dominated by low-confidence noise.
*   **Threshold Sensitivity:** Demonstrates the drop-off as minimum edge weight is raised. 
    *   *Weight 20:* "Reliable Skeleton" (7.5% of edges, 25.6% of nodes).
    *   *Weight 100:* Undeniable core, but too sparse for pathfinding.
    *   **The Brain's Sweet Spot:** Pathfinding minimum threshold should likely be tuned to **weight 5-10**.

#### **Hub Nodes Tab**
Top 20 skills by degree (connectivity).
*   **Gravity Distortion:** 10 of the top 20 most-connected nodes are suspected false positives (e.g., *gnu make* having more connections than *Python*).
*   **Suspects:** "gnu make", "identity", "futures", and "dfat" dominate due to natural language matching in non-technical contexts.

#### **Anomaly Detection Tab**
Two-layer analysis (Global vs. Per-Group Z-scores):
*   **41 Likely False Positives:** High Z-score globally and within group.
*   **12 Group Anomalies:** Global outliers hidden within specific groups (e.g., "Unity" at 6.19σ in Game Engines due to "team unity" boilerplate).
*   **12 Legitimate Hubs:** High global frequency but normal among peers (e.g., Python, AWS, React).
*   **2 Suspect (Small Group):** Insufficient group data to confirm or deny global suspicion.

---

### 2. Skill Occurrence Anomaly Detection

A specialized interactive tool for isolating frequency-based outliers.

*   **Sliding Window Analysis:**
    *   **Lower Threshold:** Controls the "Investigate" sensitivity multiplier (σ).
    *   **Upper Threshold:** Automatically locked at **Lower + 1.0σ** to define the "Noise/False Positive" boundary.
*   **Interactive Distribution Plot:** Histogram of log-transformed counts with real-time Gaussian fit.
*   **Bin Inspector:** Click any bar to see the exact skills and their raw occurrence counts in that frequency range.
*   **Data Explorer:** A searchable table comparing **Global σ** vs. **Group σ** to isolate semantic anomalies (e.g., "rest" being an outlier in Web Ecosystem).

---

## Development

### Technical Stack
*   **Frontend:** React (TypeScript), D3.js, Vanilla CSS.
*   **Analytics:** Python (Statistics, Math, Collections).
*   **Data:** JSON-based co-occurrence matrices.

### Setup
1. `npm install`
2. `python3 analyze_anomalies.py` (to generate fresh reports)
3. `npm run dev`
