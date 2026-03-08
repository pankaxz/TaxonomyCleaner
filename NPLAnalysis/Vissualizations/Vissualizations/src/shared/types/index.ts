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
