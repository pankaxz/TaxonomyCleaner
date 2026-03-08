#!/bin/bash
# Run from your project root:
# /mnt/workspace/DataFactoryServices/NPLAnalysis/Vissualizations/Vissualizations

# ─── shared/types/index.ts ──────────────────────────────────────────────────
cat > src/shared/types/index.ts << 'ENDOFFILE'
export interface SkillNode {
  total_count: number;
  group: string;
  super_group: string;
  senior_count?: number;
}

export interface GraphEdge {
  source: string;
  target: string;
  weight: number;
}

export interface UniverseData {
  metadata: {
    total_jds_processed: number;
    total_nodes: number;
    total_edges: number;
  };
  nodes: Record<string, SkillNode>;
  links: GraphEdge[];
}

export interface AnomalyReport {
  summary: {
    total_nodes: number;
    connected_nodes: number;
    disconnected_nodes: number;
    log_mean: number;
    log_stdev: number;
    sigma_threshold: number;
    flagged_count: number;
    flagged_high: number;
    flagged_low: number;
  };
  flagged_skills: FlaggedSkill[];
}

export interface FlaggedSkill {
  skill: string;
  total_count: number;
  log_count: number;
  z_score: number;
  group: string;
  super_group: string;
  direction: "high" | "low";
  group_z_score?: number;
  classification?: "likely_false_positive" | "group_anomaly" | "legitimate_hub" | "suspect_global_only";
}

export interface WeightDistBucket {
  range: string;
  count: number;
  pct: number;
}

export interface ThresholdBreakdown {
  min_weight: number;
  edges: number;
  edges_pct: number;
  nodes: number;
  nodes_pct: number;
}

export interface GraphAudit {
  overview: {
    total_jds: number;
    total_nodes: number;
    total_edges: number;
    connected_nodes: number;
    isolated_nodes: number;
    zero_occurrence_nodes: number;
    connected_ratio: number;
    avg_weight: number;
    median_weight: number;
    max_weight: number;
    min_weight: number;
  };
  weight_distribution: WeightDistBucket[];
  threshold_breakdown: ThresholdBreakdown[];
  top_edges: {
    source: string;
    target: string;
    weight: number;
    source_group: string;
    target_group: string;
  }[];
  node_degree_stats: {
    avg_degree: number;
    median_degree: number;
    max_degree: number;
    top_by_degree: { skill: string; degree: number; group: string }[];
    top_by_weighted_degree: { skill: string; weighted_degree: number; group: string }[];
  };
}

export type HealthStatus = "healthy" | "warning" | "critical";
ENDOFFILE

# ─── shared/hooks/index.ts ──────────────────────────────────────────────────
cat > src/shared/hooks/index.ts << 'ENDOFFILE'
// Shared hooks — planned:
// - useUniverseData()
// - useAnomalyReport()
// - useGraphAudit()
ENDOFFILE

# ─── shared/components/StatCard.tsx ──────────────────────────────────────────
cat > src/shared/components/StatCard.tsx << 'ENDOFFILE'
interface StatCardProps {
  value: string | number;
  label: string;
  sub?: string;
  accent?: string;
  warn?: boolean;
}

export function StatCard({ value, label, sub, accent = "#7dd3fc", warn = false }: StatCardProps) {
  return (
    <div style={{
      background: "#111a20",
      border: `1px solid ${warn ? "#f87171" : accent}18`,
      borderRadius: 10,
      padding: "16px 20px",
      flex: 1,
      minWidth: 130,
    }}>
      <div style={{
        fontSize: 30, fontWeight: 700,
        color: warn ? "#f87171" : accent,
        lineHeight: 1.1,
        fontVariantNumeric: "tabular-nums",
      }}>
        {typeof value === "number" ? value.toLocaleString() : value}
      </div>
      <div style={{ fontSize: 12, color: "#8a9caa", marginTop: 4, fontWeight: 500 }}>{label}</div>
      {sub && <div style={{ fontSize: 10, color: "#4a6474", marginTop: 2 }}>{sub}</div>}
    </div>
  );
}
ENDOFFILE

# ─── shared/components/HealthIndicator.tsx ───────────────────────────────────
cat > src/shared/components/HealthIndicator.tsx << 'ENDOFFILE'
import type { HealthStatus } from "../types";

const STATUS_COLORS: Record<HealthStatus, string> = {
  healthy: "#4ade80",
  warning: "#fbbf24",
  critical: "#f87171",
};

interface HealthIndicatorProps {
  status: HealthStatus;
  label: string;
}

export function HealthIndicator({ status, label }: HealthIndicatorProps) {
  const color = STATUS_COLORS[status];
  return (
    <span style={{
      display: "inline-flex", alignItems: "center", gap: 6,
      padding: "4px 12px", borderRadius: 20, fontSize: 11,
      fontWeight: 700, background: color + "14", color: color,
      border: `1px solid ${color}30`,
      textTransform: "uppercase", letterSpacing: "0.3px",
    }}>
      <span style={{
        width: 7, height: 7, borderRadius: "50%",
        background: color, boxShadow: `0 0 6px ${color}60`,
      }} />
      {label}
    </span>
  );
}
ENDOFFILE

# ─── shared/components/SectionHeader.tsx ─────────────────────────────────────
cat > src/shared/components/SectionHeader.tsx << 'ENDOFFILE'
interface SectionHeaderProps {
  title: string;
  subtitle?: string;
}

export function SectionHeader({ title, subtitle }: SectionHeaderProps) {
  return (
    <div style={{ marginBottom: 16, marginTop: 32 }}>
      <h2 style={{ fontSize: 16, fontWeight: 700, color: "#e0ecf2", margin: 0, letterSpacing: "-0.3px" }}>
        {title}
      </h2>
      {subtitle && <p style={{ fontSize: 12, color: "#5a7a88", margin: "4px 0 0" }}>{subtitle}</p>}
    </div>
  );
}
ENDOFFILE

# ─── shared/components/index.ts ──────────────────────────────────────────────
cat > src/shared/components/index.ts << 'ENDOFFILE'
export { StatCard } from "./StatCard";
export { HealthIndicator } from "./HealthIndicator";
export { SectionHeader } from "./SectionHeader";
ENDOFFILE

# ─── panels/OverviewPanel.tsx ────────────────────────────────────────────────
cat > src/dashboards/TaxonomyHealthDashboard/panels/OverviewPanel.tsx << 'ENDOFFILE'
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
ENDOFFILE

# ─── panels/GroupBalancePanel.tsx ────────────────────────────────────────────
cat > src/dashboards/TaxonomyHealthDashboard/panels/GroupBalancePanel.tsx << 'ENDOFFILE'
import { useRef, useEffect } from "react";
import * as d3 from "d3";
import { SectionHeader } from "../../../shared/components";

interface GroupData { name: string; groups: number; color: string; skills_example: string; }
interface GroupBalancePanelProps { data: GroupData[]; }

export function GroupBalancePanel({ data }: GroupBalancePanelProps) {
  const ref = useRef<SVGSVGElement>(null);
  useEffect(() => {
    if (!ref.current) return;
    const svg = d3.select(ref.current);
    svg.selectAll("*").remove();
    const margin = { top: 8, right: 16, bottom: 24, left: 90 };
    const width = 420 - margin.left - margin.right;
    const height = 360 - margin.top - margin.bottom;
    const sorted = [...data].sort((a, b) => b.groups - a.groups);
    const g = svg.attr("width", width + margin.left + margin.right).attr("height", height + margin.top + margin.bottom)
      .append("g").attr("transform", `translate(${margin.left},${margin.top})`);
    const maxG = d3.max(sorted, d => d.groups) ?? 40;
    const x = d3.scaleLinear().domain([0, maxG + 5]).range([0, width]);
    const y = d3.scaleBand().domain(sorted.map(d => d.name)).range([0, height]).padding(0.28);
    g.selectAll("rect").data(sorted).join("rect")
      .attr("x", 0).attr("y", d => y(d.name)!).attr("width", d => x(d.groups))
      .attr("height", y.bandwidth()).attr("fill", d => d.color).attr("opacity", 0.75).attr("rx", 4);
    g.selectAll(".label").data(sorted).join("text")
      .attr("x", d => x(d.groups) + 6).attr("y", d => y(d.name)! + y.bandwidth() / 2 + 4)
      .attr("fill", "#8a9caa").attr("font-size", "11px").attr("font-family", "inherit").text(d => d.groups);
    g.selectAll(".name").data(sorted).join("text")
      .attr("x", -6).attr("y", d => y(d.name)! + y.bandwidth() / 2 + 4).attr("text-anchor", "end")
      .attr("fill", d => d.color).attr("font-size", "11px").attr("font-weight", "600").attr("font-family", "inherit").text(d => d.name);
    g.append("g").attr("transform", `translate(0,${height})`).call(d3.axisBottom(x).ticks(5))
      .selectAll("text").attr("fill", "#5a7a88").attr("font-size", "10px").attr("font-family", "inherit");
    g.selectAll(".domain").attr("stroke", "#2a3a48");
    g.selectAll(".tick line").attr("stroke", "#1a2a38");
  }, [data]);

  return (
    <div>
      <SectionHeader title="Super-Group Balance" subtitle="Number of sub-groups per super-group" />
      <div style={{ background: "#111a20", borderRadius: 10, padding: "16px", border: "1px solid #1a2a36" }}>
        <svg ref={ref} />
      </div>
    </div>
  );
}
ENDOFFILE

# ─── panels/EdgeQualityPanel.tsx ─────────────────────────────────────────────
cat > src/dashboards/TaxonomyHealthDashboard/panels/EdgeQualityPanel.tsx << 'ENDOFFILE'
import { useRef, useEffect } from "react";
import * as d3 from "d3";
import { SectionHeader, StatCard } from "../../../shared/components";
import type { WeightDistBucket, ThresholdBreakdown } from "../../../shared/types";

interface EdgeQualityPanelProps { weightDist: WeightDistBucket[]; thresholds: ThresholdBreakdown[]; }

export function EdgeQualityPanel({ weightDist, thresholds }: EdgeQualityPanelProps) {
  const distRef = useRef<SVGSVGElement>(null);
  const threshRef = useRef<SVGSVGElement>(null);

  useEffect(() => {
    if (!distRef.current) return;
    const svg = d3.select(distRef.current); svg.selectAll("*").remove();
    const margin = { top: 8, right: 16, bottom: 40, left: 54 };
    const width = 420 - margin.left - margin.right;
    const height = 240 - margin.top - margin.bottom;
    const g = svg.attr("width", width + margin.left + margin.right).attr("height", height + margin.top + margin.bottom)
      .append("g").attr("transform", `translate(${margin.left},${margin.top})`);
    const x = d3.scaleBand().domain(weightDist.map(d => d.range)).range([0, width]).padding(0.2);
    const y = d3.scaleLog().domain([10, 50000]).range([height, 0]).clamp(true);
    g.selectAll("rect").data(weightDist).join("rect")
      .attr("x", d => x(d.range)!).attr("width", x.bandwidth())
      .attr("y", d => y(d.count)).attr("height", d => height - y(d.count))
      .attr("fill", (_d, i) => i === 0 ? "#f87171" : i <= 1 ? "#fb923c" : i <= 3 ? "#fbbf24" : "#4ade80")
      .attr("opacity", 0.8).attr("rx", 3);
    g.selectAll(".pct").data(weightDist).join("text")
      .attr("x", d => x(d.range)! + x.bandwidth() / 2).attr("y", d => y(d.count) - 5)
      .attr("text-anchor", "middle").attr("fill", "#8a9caa").attr("font-size", "9px").attr("font-family", "inherit")
      .text(d => d.pct > 0.1 ? d.pct.toFixed(1) + "%" : "");
    g.append("g").attr("transform", `translate(0,${height})`).call(d3.axisBottom(x))
      .selectAll("text").attr("fill", "#5a7a88").attr("font-size", "9px").attr("font-family", "inherit")
      .attr("transform", "rotate(-30)").attr("text-anchor", "end");
    g.append("g").call(d3.axisLeft(y).ticks(4, ",.0f"))
      .selectAll("text").attr("fill", "#5a7a88").attr("font-size", "10px").attr("font-family", "inherit");
    g.selectAll(".domain").attr("stroke", "#2a3a48"); g.selectAll(".tick line").attr("stroke", "#1a2a38");
    g.append("text").attr("x", width / 2).attr("y", height + 36).attr("text-anchor", "middle")
      .attr("fill", "#4a6474").attr("font-size", "10px").attr("font-family", "inherit").text("Edge weight range");
  }, [weightDist]);

  useEffect(() => {
    if (!threshRef.current) return;
    const svg = d3.select(threshRef.current); svg.selectAll("*").remove();
    const margin = { top: 12, right: 50, bottom: 36, left: 50 };
    const width = 420 - margin.left - margin.right;
    const height = 220 - margin.top - margin.bottom;
    const g = svg.attr("width", width + margin.left + margin.right).attr("height", height + margin.top + margin.bottom)
      .append("g").attr("transform", `translate(${margin.left},${margin.top})`);
    const x = d3.scaleLog().domain([1, 100]).range([0, width]);
    const yS = d3.scaleLinear().domain([0, 100]).range([height, 0]);
    const eLine = d3.line<ThresholdBreakdown>().x(d => x(d.min_weight)).y(d => yS(d.edges_pct)).curve(d3.curveMonotoneX);
    const nLine = d3.line<ThresholdBreakdown>().x(d => x(d.min_weight)).y(d => yS(d.nodes_pct)).curve(d3.curveMonotoneX);
    const eArea = d3.area<ThresholdBreakdown>().x(d => x(d.min_weight)).y0(height).y1(d => yS(d.edges_pct)).curve(d3.curveMonotoneX);
    const nArea = d3.area<ThresholdBreakdown>().x(d => x(d.min_weight)).y0(height).y1(d => yS(d.nodes_pct)).curve(d3.curveMonotoneX);
    g.append("path").datum(thresholds).attr("fill", "#7dd3fc10").attr("d", eArea);
    g.append("path").datum(thresholds).attr("fill", "#c4b5fd10").attr("d", nArea);
    g.append("path").datum(thresholds).attr("fill", "none").attr("stroke", "#7dd3fc").attr("stroke-width", 2.5).attr("d", eLine);
    g.append("path").datum(thresholds).attr("fill", "none").attr("stroke", "#c4b5fd").attr("stroke-width", 2.5).attr("d", nLine);
    g.selectAll(".ed").data(thresholds).join("circle").attr("cx", d => x(d.min_weight)).attr("cy", d => yS(d.edges_pct)).attr("r", 3.5).attr("fill", "#7dd3fc");
    g.selectAll(".nd").data(thresholds).join("circle").attr("cx", d => x(d.min_weight)).attr("cy", d => yS(d.nodes_pct)).attr("r", 3.5).attr("fill", "#c4b5fd");
    g.append("g").attr("transform", `translate(0,${height})`).call(d3.axisBottom(x).tickValues([1,2,5,10,20,50,100]).tickFormat(d3.format("d")))
      .selectAll("text").attr("fill", "#5a7a88").attr("font-size", "10px").attr("font-family", "inherit");
    g.append("g").call(d3.axisLeft(yS).ticks(5).tickFormat(d => d + "%"))
      .selectAll("text").attr("fill", "#5a7a88").attr("font-size", "10px").attr("font-family", "inherit");
    g.selectAll(".domain").attr("stroke", "#2a3a48"); g.selectAll(".tick line").attr("stroke", "#1a2a38");
    g.append("text").attr("x", width / 2).attr("y", height + 32).attr("text-anchor", "middle")
      .attr("fill", "#4a6474").attr("font-size", "10px").attr("font-family", "inherit").text("Minimum edge weight threshold");
    [{label:"Edges retained",color:"#7dd3fc",y:4},{label:"Nodes retained",color:"#c4b5fd",y:18}].forEach(l => {
      g.append("line").attr("x1",width-110).attr("x2",width-90).attr("y1",l.y).attr("y2",l.y).attr("stroke",l.color).attr("stroke-width",2.5);
      g.append("text").attr("x",width-86).attr("y",l.y+4).attr("fill",l.color).attr("font-size","10px").attr("font-family","inherit").text(l.label);
    });
  }, [thresholds]);

  const noiseEdges = weightDist[0]?.pct ?? 0;
  const lowPct = weightDist.slice(0, 2).reduce((s, d) => s + d.pct, 0);
  const reliablePct = thresholds.find(t => t.min_weight === 20)?.edges_pct ?? 0;
  const strongPct = thresholds.find(t => t.min_weight === 100)?.edges_pct ?? 0;

  return (
    <div>
      <SectionHeader title="Edge Weight Distribution" subtitle="How many JDs back each skill-pair co-occurrence" />
      <div style={{ display: "flex", gap: 20, flexWrap: "wrap", marginBottom: 24 }}>
        <div style={{ background: "#111a20", borderRadius: 10, padding: "16px", border: "1px solid #1a2a36", flex: "1 1 440px" }}>
          <svg ref={distRef} />
        </div>
        <div style={{ flex: "1 1 280px", display: "flex", flexDirection: "column", gap: 10 }}>
          <StatCard value={`${noiseEdges.toFixed(1)}%`} label="Weight = 1" sub="noise" accent="#f87171" warn />
          <StatCard value={`${lowPct.toFixed(1)}%`} label="Weight ≤ 4" sub="< 5 JDs" accent="#fb923c" warn />
          <StatCard value={`${reliablePct.toFixed(1)}%`} label="Weight ≥ 20" sub="reliable" accent="#4ade80" />
          <StatCard value={`${strongPct.toFixed(1)}%`} label="Weight ≥ 100" sub="strong" accent="#38bdf8" />
        </div>
      </div>
      <SectionHeader title="Threshold Sensitivity" subtitle="What survives at different minimum edge weights" />
      <div style={{ background: "#111a20", borderRadius: 10, padding: "16px", border: "1px solid #1a2a36" }}>
        <svg ref={threshRef} />
      </div>
    </div>
  );
}
ENDOFFILE

# ─── panels/HubNodesPanel.tsx ────────────────────────────────────────────────
cat > src/dashboards/TaxonomyHealthDashboard/panels/HubNodesPanel.tsx << 'ENDOFFILE'
import { SectionHeader } from "../../../shared/components";

interface HubNode { skill: string; degree: number; group: string; super_group: string; suspect: boolean; }
interface HubNodesPanelProps { nodes: HubNode[]; maxDegree: number; }

export function HubNodesPanel({ nodes, maxDegree }: HubNodesPanelProps) {
  const suspectCount = nodes.filter(n => n.suspect).length;
  return (
    <div>
      <SectionHeader title={`Top ${nodes.length} Hub Nodes by Degree`} subtitle="Red = suspected false positives from common English words" />
      <div style={{ background: "#111a20", borderRadius: 10, border: "1px solid #1a2a36", overflow: "hidden" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
          <thead><tr style={{ borderBottom: "1px solid #1a2e38" }}>
            {["#","Skill","Degree","Group","Status"].map(h => (
              <th key={h} style={{ padding: "8px 12px", textAlign: "left", color: "#4a6a78", fontWeight: 600, fontSize: 10, textTransform: "uppercase" }}>{h}</th>
            ))}
          </tr></thead>
          <tbody>{nodes.map((s, i) => (
            <tr key={s.skill} style={{ borderBottom: "1px solid #111a22", background: i % 2 === 0 ? "#111a20" : "#131d24" }}>
              <td style={{ padding: "7px 12px", color: "#4a6a78" }}>{i + 1}</td>
              <td style={{ padding: "7px 12px", fontWeight: 600, color: s.suspect ? "#f87171" : "#d0e0e8" }}>{s.skill}</td>
              <td style={{ padding: "7px 12px", fontVariantNumeric: "tabular-nums" }}>
                <span style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
                  {s.degree.toLocaleString()}
                  <span style={{ width: Math.round(s.degree / maxDegree * 60), height: 5, borderRadius: 3, display: "inline-block",
                    background: s.suspect ? "linear-gradient(90deg, #f87171, #f8717188)" : "linear-gradient(90deg, #4ade80, #4ade8088)" }} />
                </span>
              </td>
              <td style={{ padding: "7px 12px", color: "#6a8a98", fontSize: 10 }}>{s.super_group || s.group}</td>
              <td style={{ padding: "7px 12px" }}>
                <span style={{ display: "inline-block", padding: "2px 8px", borderRadius: 10, fontSize: 9, fontWeight: 700, textTransform: "uppercase",
                  background: s.suspect ? "#f8717115" : "#4ade8015", color: s.suspect ? "#f87171" : "#4ade80",
                  border: `1px solid ${s.suspect ? "#f8717130" : "#4ade8030"}` }}>
                  {s.suspect ? "Suspect" : "Legitimate"}
                </span>
              </td>
            </tr>
          ))}</tbody>
        </table>
      </div>
      <div style={{ marginTop: 16, background: "#111a20", borderRadius: 10, padding: "14px 20px", border: "1px solid #1a2a36", fontSize: 12, color: "#8a9caa" }}>
        <span style={{ color: "#f87171", fontWeight: 600 }}>{suspectCount} of {nodes.length}</span> hub nodes are suspected false positives.
      </div>
    </div>
  );
}
ENDOFFILE

# ─── panels/AnomalyDetectionPanel.tsx ────────────────────────────────────────
cat > src/dashboards/TaxonomyHealthDashboard/panels/AnomalyDetectionPanel.tsx << 'ENDOFFILE'
import { SectionHeader } from "../../../shared/components";

interface AnomalyCounts { likely_false_positive: number; group_anomaly: number; legitimate_hub: number; suspect_global_only: number; }
interface FPEntry { skill: string; count: number | null; global_z: number | null; group_z: number | null; trigger: string; }
interface AnomalyDetectionPanelProps { counts: AnomalyCounts; topFalsePositives: FPEntry[]; }

export function AnomalyDetectionPanel({ counts, topFalsePositives }: AnomalyDetectionPanelProps) {
  const categories = [
    { label: "Likely False Positive", count: counts.likely_false_positive, color: "#f87171", desc: "High global + high in-group z-score" },
    { label: "Group Anomaly", count: counts.group_anomaly, color: "#fb923c", desc: "Normal global, extreme in-group outlier" },
    { label: "Legitimate Hub", count: counts.legitimate_hub, color: "#4ade80", desc: "High global, normal among peers" },
    { label: "Suspect (small group)", count: counts.suspect_global_only, color: "#fbbf24", desc: "Group too small for per-group stats" },
  ];

  return (
    <div>
      <SectionHeader title="Two-Layer Anomaly Detection" subtitle="Global log-normal + per-group z-scores" />
      <div style={{ display: "flex", gap: 10, marginBottom: 16, flexWrap: "wrap" }}>
        {categories.map(c => (
          <div key={c.label} style={{ flex: 1, minWidth: 140, background: c.color + "0a", border: `1px solid ${c.color}20`, borderRadius: 8, padding: "12px 16px" }}>
            <div style={{ fontSize: 26, fontWeight: 700, color: c.color }}>{c.count}</div>
            <div style={{ fontSize: 11, fontWeight: 600, color: c.color, opacity: 0.85 }}>{c.label}</div>
            <div style={{ fontSize: 9, color: "#5a7a88", marginTop: 3 }}>{c.desc}</div>
          </div>
        ))}
      </div>
      <div style={{ background: "#111a20", borderRadius: 8, border: "1px solid #1a2a36", overflow: "hidden" }}>
        <div style={{ padding: "10px 14px", borderBottom: "1px solid #1a2a36", fontSize: 11, fontWeight: 600, color: "#8a9caa" }}>
          Top False Positives — Requires Action
        </div>
        <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
          <thead><tr style={{ borderBottom: "1px solid #1a2a36" }}>
            {["Skill","Count","Global σ","Group σ","Probable Trigger"].map(h => (
              <th key={h} style={{ padding: "7px 12px", textAlign: "left", color: "#4a6a78", fontWeight: 600, fontSize: 9, textTransform: "uppercase" }}>{h}</th>
            ))}
          </tr></thead>
          <tbody>{topFalsePositives.map((s, i) => (
            <tr key={s.skill} style={{ borderBottom: "1px solid #0e161c", background: i % 2 === 0 ? "#111a20" : "#131d24" }}>
              <td style={{ padding: "6px 12px", fontWeight: 600, color: "#f87171" }}>{s.skill}</td>
              <td style={{ padding: "6px 12px", fontVariantNumeric: "tabular-nums", color: "#c0d0d8" }}>{s.count?.toLocaleString() ?? "—"}</td>
              <td style={{ padding: "6px 12px", fontVariantNumeric: "tabular-nums", color: "#fb923c", fontWeight: 600 }}>{s.global_z?.toFixed(2) ?? "—"}</td>
              <td style={{ padding: "6px 12px", fontVariantNumeric: "tabular-nums", color: s.group_z ? "#f87171" : "#4a6a78", fontWeight: s.group_z ? 700 : 400 }}>{s.group_z?.toFixed(2) ?? "—"}</td>
              <td style={{ padding: "6px 12px", color: "#7a9aa8", fontStyle: "italic", fontSize: 10 }}>{s.trigger}</td>
            </tr>
          ))}</tbody>
        </table>
      </div>
    </div>
  );
}
ENDOFFILE

# ─── panels/index.ts ─────────────────────────────────────────────────────────
cat > src/dashboards/TaxonomyHealthDashboard/panels/index.ts << 'ENDOFFILE'
export { OverviewPanel } from "./OverviewPanel";
export { GroupBalancePanel } from "./GroupBalancePanel";
export { EdgeQualityPanel } from "./EdgeQualityPanel";
export { HubNodesPanel } from "./HubNodesPanel";
export { AnomalyDetectionPanel } from "./AnomalyDetectionPanel";
ENDOFFILE

# ─── Copy audit data ─────────────────────────────────────────────────────────
if [ -f src/dashboards/SkillOccurrenceAnomalyDetection/data/audit_graph_audit.json ]; then
  cp src/dashboards/SkillOccurrenceAnomalyDetection/data/audit_graph_audit.json src/dashboards/TaxonomyHealthDashboard/data/
  echo "✓ Copied audit_graph_audit.json to TaxonomyHealthDashboard/data/"
else
  echo "⚠ audit_graph_audit.json not found — copy it manually to src/dashboards/TaxonomyHealthDashboard/data/"
fi

echo ""
echo "✓ All scaffold files created. Run 'npm run dev' to verify."
