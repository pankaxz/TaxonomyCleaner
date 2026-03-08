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
