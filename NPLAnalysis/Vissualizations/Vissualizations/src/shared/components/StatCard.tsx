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
