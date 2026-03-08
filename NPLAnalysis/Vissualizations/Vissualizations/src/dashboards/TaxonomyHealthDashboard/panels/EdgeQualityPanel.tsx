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
