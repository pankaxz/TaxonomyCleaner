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
