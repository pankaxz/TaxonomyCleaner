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
