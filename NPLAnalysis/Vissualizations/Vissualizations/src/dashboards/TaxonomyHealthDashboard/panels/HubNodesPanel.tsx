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
