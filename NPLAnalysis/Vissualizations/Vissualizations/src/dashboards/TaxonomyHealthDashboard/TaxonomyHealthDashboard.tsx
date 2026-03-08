import { useState } from "react";
import { HealthIndicator } from "../../shared/components";
import { OverviewPanel, GroupBalancePanel, EdgeQualityPanel, HubNodesPanel, AnomalyDetectionPanel } from "./panels";
import type { GraphAudit, HealthStatus } from "../../shared/types";

// ─── DATA ───────────────────────────────────────────────────────────────────
// TODO: Replace with dynamic JSON imports from data/ directory.
// After each pipeline run:
//   1. Run analyze_anomalies.py → writes anomaly_report.json
//   2. Run graph audit script → writes audit_graph_audit.json
//   3. Dashboard reads both files automatically.

import auditData from "./data/audit_graph_audit.json";
// import anomalyData from "./data/anomaly_report.json";

// ─── STATIC CONFIG (derived from taxonomy) ──────────────────────────────────

const SUPER_GROUP_DATA = [
  { name: "Backend", groups: 37, color: "#6ee7b7", skills_example: "Languages, Databases, Architecture" },
  { name: "Cloud", groups: 30, color: "#7dd3fc", skills_example: "Cloud Computing, DevOps, Security" },
  { name: "Frontend", groups: 18, color: "#c4b5fd", skills_example: "Frontend Dev, Mobile, CSS" },
  { name: "DevTools", groups: 13, color: "#fbbf24", skills_example: "Dev Tools, Package Mgmt, CLI" },
  { name: "Business", groups: 13, color: "#f9a8d4", skills_example: "Project Mgmt, Communication" },
  { name: "AI", groups: 12, color: "#f87171", skills_example: "Data Science, NLP, Computer Vision" },
  { name: "Testing", groups: 12, color: "#a3e635", skills_example: "QA, Performance Testing" },
  { name: "Embedded", groups: 10, color: "#fb923c", skills_example: "Hardware, IoT, CAD" },
  { name: "Data", groups: 7, color: "#38bdf8", skills_example: "Data Eng, Streaming, Viz" },
  { name: "Gaming", groups: 3, color: "#e879f9", skills_example: "Game Engines, XR" },
  { name: "Blockchain", groups: 2, color: "#facc15", skills_example: "Blockchain, Frameworks" },
  { name: "Healthcare", groups: 2, color: "#4ade80", skills_example: "Healthcare Platforms" },
  { name: "Deprecated", groups: 1, color: "#64748b", skills_example: "Deprecated Technologies" },
];

// Hub nodes derived from audit top_by_degree + anomaly classification
const HUB_NODES = (auditData as GraphAudit).node_degree_stats.top_by_degree.map((n) => ({
  skill: n.skill,
  degree: n.degree,
  group: n.group,
  super_group: "", // Would come from universe.json node data
  suspect: [
    "gnu make", "identity", "futures", "dfat", "apis",
    "training", "data analysis", "rest", "pnr", "go",
  ].includes(n.skill),
}));

// Anomaly counts from the two-layer analysis
const ANOMALY_COUNTS = {
  likely_false_positive: 41,
  group_anomaly: 12,
  legitimate_hub: 12,
  suspect_global_only: 2,
};

const TOP_FALSE_POSITIVES = [
  { skill: "gnu make", count: 1845, global_z: 3.01, group_z: null, trigger: '"make" in natural language' },
  { skill: "identity", count: 1491, global_z: 2.90, group_z: null, trigger: '"identity" in generic context' },
  { skill: "futures", count: 1358, global_z: 2.84, group_z: null, trigger: '"future/futures" as generic noun' },
  { skill: "dfat", count: 1149, global_z: 2.75, group_z: null, trigger: "Suspicious — needs investigation" },
  { skill: "training", count: 866, global_z: 2.59, group_z: null, trigger: '"training" as employee training' },
  { skill: "go", count: 749, global_z: 2.51, group_z: null, trigger: '"go" in natural language' },
  { skill: "coverage", count: 628, global_z: 2.41, group_z: null, trigger: '"coverage" as generic noun' },
  { skill: "d", count: 600, global_z: 2.39, group_z: null, trigger: "Single letter matching" },
  { skill: "foundation", count: 432, global_z: 2.21, group_z: 5.23, trigger: '"strong foundation in..."' },
  { skill: "move", count: 479, global_z: 2.26, group_z: null, trigger: '"move" as generic verb' },
  { skill: "accelerate", count: 436, global_z: 2.21, group_z: null, trigger: '"accelerate" as generic verb' },
  { skill: "unity", count: null, global_z: null, group_z: 6.19, trigger: '"team unity" in Game Engines' },
];

// ─── DASHBOARD ──────────────────────────────────────────────────────────────

const TABS = [
  { key: "overview", label: "Overview" },
  { key: "groups", label: "Group Balance" },
  { key: "edges", label: "Edge Quality" },
  { key: "hubs", label: "Hub Nodes" },
  { key: "anomalies", label: "Anomaly Detection" },
] as const;

type TabKey = typeof TABS[number]["key"];

export function TaxonomyHealthDashboard() {
  const [tab, setTab] = useState<TabKey>("overview");

  const audit = auditData as GraphAudit;
  const { overview, weight_distribution, threshold_breakdown } = audit;

  const coverageHealth: HealthStatus =
    overview.connected_ratio > 0.7 ? "healthy" : overview.connected_ratio > 0.5 ? "warning" : "critical";
  const noiseHealth: HealthStatus =
    weight_distribution[0].pct < 30 ? "healthy" : weight_distribution[0].pct < 40 ? "warning" : "critical";
  const fpHealth: HealthStatus =
    ANOMALY_COUNTS.likely_false_positive < 10 ? "healthy" : ANOMALY_COUNTS.likely_false_positive < 25 ? "warning" : "critical";

  return (
    <div>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 24 }}>
        <div>
          <h1 style={{ fontSize: 22, fontWeight: 700, color: "#e8f2f6", margin: 0, letterSpacing: "-0.5px" }}>
            Taxonomy Health Dashboard
          </h1>
          <p style={{ fontSize: 12, color: "#5a7a88", margin: "5px 0 0" }}>
            {overview.total_jds.toLocaleString()} JDs · {overview.total_nodes.toLocaleString()} nodes ·{" "}
            {overview.total_edges.toLocaleString()} edges
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
          <HealthIndicator status={coverageHealth} label={`Coverage ${(overview.connected_ratio * 100).toFixed(0)}%`} />
          <HealthIndicator status={noiseHealth} label={`Noise ${weight_distribution[0].pct.toFixed(0)}%`} />
          <HealthIndicator status={fpHealth} label={`${ANOMALY_COUNTS.likely_false_positive} false pos`} />
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 2, marginBottom: 24, borderBottom: "1px solid #1a2a36" }}>
        {TABS.map((t) => (
          <button
            key={t.key}
            onClick={() => setTab(t.key)}
            style={{
              background: "transparent",
              border: "none",
              borderBottom: tab === t.key ? "2px solid #7dd3fc" : "2px solid transparent",
              padding: "10px 18px",
              color: tab === t.key ? "#e0ecf2" : "#4a6a78",
              fontSize: 12,
              fontWeight: 600,
              cursor: "pointer",
              fontFamily: "inherit",
              transition: "color 0.15s",
            }}
          >
            {t.label}
          </button>
        ))}
      </div>

      {/* Panel content */}
      {tab === "overview" && <OverviewPanel audit={audit} anomalyCounts={ANOMALY_COUNTS} />}
      {tab === "groups" && <GroupBalancePanel data={SUPER_GROUP_DATA} />}
      {tab === "edges" && <EdgeQualityPanel weightDist={weight_distribution} thresholds={threshold_breakdown} />}
      {tab === "hubs" && <HubNodesPanel nodes={HUB_NODES} maxDegree={HUB_NODES[0]?.degree ?? 1} />}
      {tab === "anomalies" && <AnomalyDetectionPanel counts={ANOMALY_COUNTS} topFalsePositives={TOP_FALSE_POSITIVES} />}

      {/* Footer */}
      <div
        style={{
          marginTop: 32,
          paddingTop: 14,
          borderTop: "1px solid #1a2a36",
          fontSize: 10,
          color: "#3a5a68",
          display: "flex",
          justifyContent: "space-between",
        }}
      >
        <span>CareerNavigator Taxonomy Health</span>
        <span>Re-run after taxonomy fixes to track improvement</span>
      </div>
    </div>
  );
}
export default TaxonomyHealthDashboard;
