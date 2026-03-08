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
