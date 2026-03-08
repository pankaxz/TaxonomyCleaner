import { useState } from "react";
import { TaxonomyHealthDashboard, SkillOccurrenceAnomalyDetection } from "./dashboards";

const DASHBOARDS = [
  {
    key: "taxonomy-health",
    label: "Taxonomy Health",
    icon: "◆",
    component: TaxonomyHealthDashboard,
  },
  {
    key: "anomaly-detection",
    label: "Anomaly Detection",
    icon: "◈",
    component: SkillOccurrenceAnomalyDetection,
  },
  // Future dashboards:
  // { key: "graph-structure", label: "Graph Structure", icon: "◇", component: GraphStructureDashboard },
  // { key: "pipeline-report", label: "Pipeline Report", icon: "◎", component: PipelineReportDashboard },
  // { key: "coverage-analysis", label: "Coverage Analysis", icon: "◉", component: CoverageAnalysisDashboard },
] as const;

type DashboardKey = typeof DASHBOARDS[number]["key"];

function App() {
  const [active, setActive] = useState<DashboardKey>("taxonomy-health");

  const ActiveDashboard = DASHBOARDS.find((d) => d.key === active)?.component ?? TaxonomyHealthDashboard;

  return (
    <div
      style={{
        display: "flex",
        minHeight: "100vh",
        background: "#0a1014",
        fontFamily: "'JetBrains Mono', 'SF Mono', 'Fira Code', monospace",
        color: "#c0d0d8",
      }}
    >
      <link
        href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&display=swap"
        rel="stylesheet"
      />

      {/* Sidebar */}
      <nav
        style={{
          width: 220,
          minHeight: "100vh",
          background: "#0c1518",
          borderRight: "1px solid #1a2a36",
          padding: "20px 0",
          display: "flex",
          flexDirection: "column",
          flexShrink: 0,
        }}
      >
        {/* Logo / title */}
        <div style={{ padding: "0 16px 20px", borderBottom: "1px solid #1a2a36" }}>
          <div style={{ fontSize: 13, fontWeight: 700, color: "#7dd3fc", letterSpacing: "-0.3px" }}>
            CareerNavigator
          </div>
          <div style={{ fontSize: 10, color: "#3a5a68", marginTop: 2 }}>Pipeline Dashboards</div>
        </div>

        {/* Dashboard links */}
        <div style={{ padding: "12px 0", flex: 1 }}>
          {DASHBOARDS.map((d) => (
            <button
              key={d.key}
              onClick={() => setActive(d.key)}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                width: "100%",
                padding: "10px 16px",
                background: active === d.key ? "#1a2e3810" : "transparent",
                border: "none",
                borderLeft: active === d.key ? "2px solid #7dd3fc" : "2px solid transparent",
                color: active === d.key ? "#e0ecf2" : "#5a7a88",
                fontSize: 12,
                fontWeight: active === d.key ? 600 : 400,
                cursor: "pointer",
                fontFamily: "inherit",
                textAlign: "left",
                transition: "all 0.15s",
              }}
            >
              <span style={{ fontSize: 14, opacity: 0.6 }}>{d.icon}</span>
              {d.label}
            </button>
          ))}
        </div>

        {/* Footer */}
        <div style={{ padding: "12px 16px", borderTop: "1px solid #1a2a36", fontSize: 9, color: "#2a4a58" }}>
          v0.1.0 · Vite + React + D3
        </div>
      </nav>

      {/* Main content */}
      <main style={{ flex: 1, padding: "24px 28px", overflowY: "auto" }}>
        <ActiveDashboard />
      </main>
    </div>
  );
}

export default App;
