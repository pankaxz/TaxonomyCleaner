import { StatCard } from "../../../shared/components";
import type { GraphAudit, HealthStatus } from "../../../shared/types";

interface OverviewPanelProps {
  audit: GraphAudit;
  anomalyCounts: {
    likely_false_positive: number;
    group_anomaly: number;
    legitimate_hub: number;
    suspect_global_only: number;
  };
}

export function OverviewPanel({ audit, anomalyCounts }: OverviewPanelProps) {
  const { overview, weight_distribution } = audit;
  const coverageHealth: HealthStatus = overview.connected_ratio > 0.7 ? "healthy" : overview.connected_ratio > 0.5 ? "warning" : "critical";
  const edgeNoiseHealth: HealthStatus = weight_distribution[0].pct < 30 ? "healthy" : weight_distribution[0].pct < 40 ? "warning" : "critical";
  const fpHealth: HealthStatus = anomalyCounts.likely_false_positive < 10 ? "healthy" : anomalyCounts.likely_false_positive < 25 ? "warning" : "critical";

  return (
    <div>
      <div style={{ display: "flex", gap: 10, flexWrap: "wrap", marginBottom: 20 }}>
        <StatCard value={overview.total_nodes} label="Total Skills" sub="in taxonomy" accent="#7dd3fc" />
        <StatCard value={overview.connected_nodes} label="Connected" sub={`${(overview.connected_ratio * 100).toFixed(1)}% of total`} accent="#4ade80" />
        <StatCard value={overview.isolated_nodes} label="Isolated" sub="zero occurrences" accent="#fbbf24" warn />
        <StatCard value={overview.total_edges} label="Edges" sub={`median weight: ${overview.median_weight}`} accent="#c4b5fd" />
        <StatCard value={overview.total_jds} label="JDs Processed" sub="data source" accent="#38bdf8" />
      </div>
      <div style={{ background: "#111a20", borderRadius: 10, padding: "18px 20px", border: "1px solid #1a2a36", marginBottom: 20 }}>
        <div style={{ fontSize: 13, fontWeight: 600, color: "#e0ecf2", marginBottom: 12 }}>Health Summary</div>
        <div style={{ display: "flex", gap: 24, flexWrap: "wrap", fontSize: 12, lineHeight: 1.8 }}>
          <div>
            <span style={{ color: "#5a7a88" }}>Coverage: </span>
            <span style={{ color: coverageHealth === "critical" ? "#f87171" : coverageHealth === "warning" ? "#fbbf24" : "#4ade80", fontWeight: 600 }}>{(overview.connected_ratio * 100).toFixed(1)}%</span>
            <span style={{ color: "#3a5a68" }}> — {overview.isolated_nodes} skills never seen in any JD</span>
          </div>
          <div>
            <span style={{ color: "#5a7a88" }}>Edge noise: </span>
            <span style={{ color: edgeNoiseHealth === "critical" ? "#f87171" : edgeNoiseHealth === "warning" ? "#fbbf24" : "#4ade80", fontWeight: 600 }}>{weight_distribution[0].pct}% at weight 1</span>
            <span style={{ color: "#3a5a68" }}> — single co-occurrence, likely noise</span>
          </div>
          <div>
            <span style={{ color: "#5a7a88" }}>False positives: </span>
            <span style={{ color: fpHealth === "critical" ? "#f87171" : fpHealth === "warning" ? "#fbbf24" : "#4ade80", fontWeight: 600 }}>{anomalyCounts.likely_false_positive} detected</span>
            <span style={{ color: "#3a5a68" }}> — common English words inflating counts</span>
          </div>
        </div>
      </div>
      <div style={{ background: "#111a20", borderRadius: 10, padding: "16px 20px", border: "1px solid #f8717120" }}>
        <div style={{ fontSize: 13, fontWeight: 600, color: "#f87171", marginBottom: 8 }}>Action Required</div>
        <div style={{ fontSize: 12, color: "#8a9caa", lineHeight: 1.7 }}>
          <span style={{ color: "#f87171", fontWeight: 600 }}>{anomalyCounts.likely_false_positive}</span> likely false positives and <span style={{ color: "#fb923c", fontWeight: 600 }}>{anomalyCounts.group_anomaly}</span> group anomalies need taxonomy cleanup.
        </div>
      </div>
    </div>
  );
}
