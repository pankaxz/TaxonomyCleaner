import { useState, useMemo, useRef, useEffect } from "react";
import * as d3 from "d3";
import RAW_DATA from './data/anomaly_report.json'

type Classification = "false_positive" | "investigate" | "legitimate";

const COLORS: Record<Classification, string> = {
    false_positive: "#ff4d6a",
    investigate: "#f0a030",
    legitimate: "#40d89b",
};

const LABELS: Record<Classification, string> = {
    false_positive: "Likely False Positive",
    investigate: "Needs Investigation",
    legitimate: "Legitimate Skill",
};

function gaussian(x: number, mean:number, stdev: number) {
    const exp = -0.5 * Math.pow((x - mean) / stdev, 2);
    return (1 / (stdev * Math.sqrt(2 * Math.PI))) * Math.exp(exp);
}

function SkillOccurrenceAnomalyDetection() {
    const [view, setView] = useState("distribution");
    const [activeFilter, setActiveFilter] = useState<Classification | "all">("all");
    const [sigmaThreshold, setSigmaThreshold] = useState(2.0);
    const [minWeight, setMinWeight] = useState(0); // New Weight Filter
    const [searchTerm, setSearchTerm] = useState("");
    const [selectedBin, setSelectedBin] = useState<{skills: {skill: string, count: number}[], range: [number, number]} | null>(null);
    
    const svgRef = useRef<SVGSVGElement>(null);
    const tooltipRef = useRef<HTMLDivElement>(null);

    // Derived properties based on filtered weight
    const { skills, mean, stdev, totalFilteredNodes } = useMemo(() => {
        // 1. Filter by weight first
        const weightFiltered = RAW_DATA.all_skills.filter((s: any) => s.count >= minWeight);
        
        // 2. Calculate local distribution metrics for the filtered set
        const logs = weightFiltered.map((s: any) => s.log_count);
        const m = logs.length > 0 ? d3.mean(logs) || 0 : 0;
        const s_dev = logs.length > 1 ? d3.deviation(logs) || 1 : 1;

        // 3. Map to final skill objects
        const upperMult = sigmaThreshold + 1.0;
        const finalSkills = weightFiltered.map((s: any) => {
            let classification: Classification = "legitimate";
            
            // Note: Using global z_score for consistent flagging OR we could use local.
            // Let's use local z_score since we are "removing from the graph"
            const localZ = (s.log_count - m) / s_dev;

            if (localZ >= upperMult) {
                classification = "false_positive";
            } else if (localZ >= sigmaThreshold || s.group_z_score >= 2.5) {
                classification = "investigate";
            }
            return { ...s, classification, localZ };
        }).sort((a: any, b: any) => b.count - a.count);

        return {
            skills: finalSkills,
            mean: m,
            stdev: s_dev,
            totalFilteredNodes: finalSkills.length
        };
    }, [minWeight, sigmaThreshold]);

    const upperThresholdMultiplier = sigmaThreshold + 1.0;

    const filtered = useMemo(() => {
        let result = skills;
        if (activeFilter !== "all") {
            result = result.filter(s => s.classification === activeFilter);
        }
        if (searchTerm) {
            result = result.filter(s => s.skill.toLowerCase().includes(searchTerm.toLowerCase()));
        }
        return result;
    }, [skills, activeFilter, searchTerm]);

    const counts = useMemo(() => {
        const c: Record<Classification, number> = {false_positive: 0, investigate: 0, legitimate: 0};
        skills.forEach((s) => {
            c[s.classification]++;
        });
        return {
            ...c,
            flagged: skills.filter(s => s.localZ >= sigmaThreshold).length
        };
    }, [skills, sigmaThreshold]);

    useEffect(() => {
        if (view !== "distribution" || !svgRef.current) return;

        const svg = d3.select(svgRef.current);
        svg.selectAll("*").remove();

        const margin = {top: 40, right: 30, bottom: 60, left: 60};
        const width = 860 - margin.left - margin.right;
        const height = 400 - margin.top - margin.bottom;

        const g = svg
            .attr("viewBox", `0 0 ${width + margin.left + margin.right} ${height + margin.top + margin.bottom}`)
            .append("g")
            .attr("transform", `translate(${margin.left},${margin.top})`);

        const allLogCounts = skills.map(s => s.log_count);
        
        // Dynamic Domain based on minWeight
        const minLog = minWeight > 0 ? Math.log(minWeight) : 0;
        const x = d3.scaleLinear().domain([minLog, 9]).range([0, width]);
        
        const bins = d3.bin<number, number>().domain(x.domain() as [number, number]).thresholds(50)(allLogCounts);
        const maxBinLen = d3.max(bins, d => d.length) || 0;
        const y = d3.scaleLinear().domain([0, maxBinLen]).range([height, 0]).nice();

        const activeLowerVal = mean + sigmaThreshold * stdev;
        const activeUpperVal = mean + upperThresholdMultiplier * stdev;

        // Bars
        g.selectAll("rect")
            .data(bins)
            .join("rect")
            .attr("x", d => x(d.x0 ?? 0) + 1)
            .attr("width", d => Math.max(0, x(d.x1 ?? 0) - x(d.x0 ?? 0) - 1))
            .attr("y", d => y(d.length))
            .attr("height", d => height - y(d.length))
            .attr("fill", d => {
                const mid = ((d.x0 ?? 0) + (d.x1 ?? 0)) / 2;
                if (mid >= activeUpperVal) return "#ff4d6a";
                if (mid >= activeLowerVal) return "#f0a030";
                return "#2a5a6a";
            })
            .attr("opacity", 0.7)
            .attr("rx", 2)
            .style("cursor", "pointer")
            .on("mouseenter", (event: MouseEvent, d: d3.Bin<number, number>) => {
                d3.select(event.currentTarget as SVGRectElement).attr("opacity", 1).attr("stroke", "#fff").attr("stroke-width", 1);
                const tooltip = d3.select(tooltipRef.current);
                tooltip.style("opacity", 1)
                    .html(`
                        <div style="font-weight:bold">Range: ${d.x0?.toFixed(2)} - ${d.x1?.toFixed(2)}</div>
                        <div>Count: ${d.length} skills</div>
                        <div style="font-size:9px;color:#8a9aa4;margin-top:4px">Click to view list</div>
                    `)
                    .style("left", (event.pageX + 15) + "px")
                    .style("top", (event.pageY - 28) + "px");
            })
            .on("mouseleave", (event: MouseEvent) => {
                d3.select(event.currentTarget as SVGRectElement).attr("opacity", 0.7).attr("stroke", "none");
                d3.select(tooltipRef.current).style("opacity", 0);
            })
            .on("click", (_event: MouseEvent, d: d3.Bin<number, number>) => {
                const skillsInBin = skills
                    .filter(s => s.log_count >= (d.x0 ?? 0) && s.log_count < (d.x1 ?? 0))
                    .map(s => ({ skill: s.skill, count: s.count }))
                    .sort((a, b) => b.count - a.count);
                setSelectedBin({
                    skills: skillsInBin,
                    range: [d.x0 ?? 0, d.x1 ?? 0]
                });
            });

        // Gaussian Fit
        if (skills.length > 0) {
            const bin0 = bins[0];
            const binWidth = (bin0?.x1 ?? 0) - (bin0?.x0 ?? 0);
            const gaussPoints = d3.range(minLog, 9, 0.05).map(xv => ({
                x: xv,
                y: gaussian(xv, mean, stdev) * skills.length * binWidth,
            }));

            const line = d3.line<{x: number, y: number}>()
                .x(d => x(d.x))
                .y(d => y(d.y))
                .curve(d3.curveBasis);

            g.append("path")
                .datum(gaussPoints)
                .attr("fill", "none")
                .attr("stroke", "rgba(255,255,255,0.1)")
                .attr("stroke-width", 2)
                .attr("stroke-dasharray", "4,4")
                .attr("d", line);
        }

        // Lower Threshold Line
        const lowerX = x(activeLowerVal);
        if (lowerX >= 0 && lowerX <= width) {
            const lowerGroup = g.append("g");
            lowerGroup.append("line")
                .attr("x1", lowerX).attr("x2", lowerX)
                .attr("y1", -10).attr("y2", height)
                .attr("stroke", "#f0a030")
                .attr("stroke-width", 2);
                
            lowerGroup.append("text")
                .attr("x", lowerX)
                .attr("y", -15)
                .attr("text-anchor", "middle")
                .attr("fill", "#f0a030")
                .attr("font-size", "10px")
                .attr("font-weight", "bold")
                .text(`${sigmaThreshold.toFixed(2)}σ`);
        }

        // Upper Threshold Line
        const upperX = x(activeUpperVal);
        if (upperX >= 0 && upperX <= width) {
            const upperGroup = g.append("g");
            upperGroup.append("line")
                .attr("x1", upperX).attr("x2", upperX)
                .attr("y1", -10).attr("y2", height)
                .attr("stroke", "#ff4d6a")
                .attr("stroke-width", 2);
                
            upperGroup.append("text")
                .attr("x", upperX)
                .attr("y", -15)
                .attr("text-anchor", "middle")
                .attr("fill", "#ff4d6a")
                .attr("font-size", "10px")
                .attr("font-weight", "bold")
                .text(`${upperThresholdMultiplier.toFixed(2)}σ`);
        }

        // Axes
        g.append("g")
            .attr("transform", `translate(0,${height})`)
            .call(d3.axisBottom(x).ticks(10))
            .selectAll("text").attr("fill", "#8a9aa4");

        g.append("g")
            .call(d3.axisLeft(y).ticks(5))
            .selectAll("text").attr("fill", "#8a9aa4");

        g.selectAll(".domain, .tick line").attr("stroke", "#2a3a44");

    }, [view, mean, stdev, sigmaThreshold, upperThresholdMultiplier, skills, minWeight]);

    return (
        <div style={{
            minHeight: "100vh", background: "#0c1518", color: "#d0dce0",
            fontFamily: "'JetBrains Mono', monospace", padding: "32px",
        }}>
            <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet"/>
            
            <div ref={tooltipRef} style={{
                position: "absolute", padding: "8px 12px", background: "rgba(20, 30, 36, 0.95)",
                border: "1px solid #2a4a58", borderRadius: "4px", pointerEvents: "none",
                fontSize: "11px", color: "#e8f0f2", opacity: 0, zIndex: 100
            }}/>

            {/* Header Area */}
            <div style={{display: "flex", justifyContent: "space-between", marginBottom: 32, gap: 24}}>
                <div style={{flex: 1}}>
                    <h1 style={{fontSize: 22, fontWeight: 700, color: "#e8f0f2", margin: 0}}>Skill Anomaly Analytics</h1>
                    <p style={{fontSize: 13, color: "#6a8a94", marginTop: 8}}>
                        Dataset: {totalFilteredNodes.toLocaleString()} skills (Filtered at weight ≥ {minWeight})
                    </p>
                </div>

                <div style={{
                    background: "#141e24", padding: "20px", borderRadius: 10, border: "1px solid #1a2e38", width: 420,
                    display: "flex", flexDirection: "column", gap: "20px"
                }}>
                    {/* Weight Filter Slider */}
                    <div>
                        <div style={{display: "flex", justifyContent: "space-between", marginBottom: 8}}>
                            <span style={{fontSize: 11, fontWeight: "bold", color: "#8a9aa4"}}>MIN OCCURRENCE (WEIGHT)</span>
                            <span style={{fontSize: 11, color: "#40d89b", fontWeight: "bold"}}>{minWeight} JDs</span>
                        </div>
                        <input 
                            type="range" min="0" max="100" step="1" 
                            value={minWeight} 
                            onChange={(e) => setMinWeight(parseInt(e.target.value))}
                            style={{width: "100%", accentColor: "#40d89b", cursor: "pointer"}}
                        />
                    </div>

                    {/* Sigma Multiplier Slider */}
                    <div>
                        <div style={{display: "flex", justifyContent: "space-between", marginBottom: 8}}>
                            <span style={{fontSize: 11, fontWeight: "bold", color: "#8a9aa4"}}>SIGMA THRESHOLD</span>
                            <span style={{fontSize: 11, color: "#f0a030", fontWeight: "bold"}}>{sigmaThreshold.toFixed(2)}σ</span>
                        </div>
                        <input 
                            type="range" min="1.0" max="3.5" step="0.01" 
                            value={sigmaThreshold} 
                            onChange={(e) => setSigmaThreshold(parseFloat(e.target.value))}
                            style={{width: "100%", accentColor: "#f0a030", cursor: "pointer"}}
                        />
                    </div>
                </div>
            </div>

            {/* Metric Overview */}
            <div style={{display: "flex", gap: 16, marginBottom: 32}}>
                {[
                    {label: "Suspicious", value: counts.flagged, accent: "#f0a030"},
                    {label: "Probable Noise", value: counts.false_positive, accent: "#ff4d6a"},
                    {label: "Current Mean", value: mean.toFixed(3), accent: "#6a9aaa"},
                    {label: "Active Window", value: totalFilteredNodes.toLocaleString(), accent: "#40d89b"},
                ].map((m, i) => (
                    <div key={i} style={{
                        flex: 1, background: "#141e24", border: "1px solid #1a2e38", borderRadius: 8, padding: "16px 20px"
                    }}>
                        <div style={{fontSize: 24, fontWeight: 700, color: m.accent}}>{m.value}</div>
                        <div style={{fontSize: 11, color: "#8a9aa4", marginTop: 4, textTransform: "uppercase"}}>{m.label}</div>
                    </div>
                ))}
            </div>

            {/* Main Workspace */}
            <div style={{display: "flex", gap: 24, alignItems: "flex-start"}}>
                <div style={{flex: 1}}>
                    <div style={{display: "flex", gap: 8, marginBottom: 16}}>
                        <button onClick={() => setView("distribution")} style={{
                            background: view === "distribution" ? "#1a2e38" : "transparent",
                            border: `1px solid ${view === "distribution" ? "#2a4a58" : "#1a2a34"}`,
                            borderRadius: 6, padding: "8px 16px", color: view === "distribution" ? "#e8f0f2" : "#5a7a84",
                            fontSize: 11, cursor: "pointer", fontWeight: "bold"
                        }}>DISTRIBUTION PLOT</button>
                        <button onClick={() => setView("table")} style={{
                            background: view === "table" ? "#1a2e38" : "transparent",
                            border: `1px solid ${view === "table" ? "#2a4a58" : "#1a2a34"}`,
                            borderRadius: 6, padding: "8px 16px", color: view === "table" ? "#e8f0f2" : "#5a7a84",
                            fontSize: 11, cursor: "pointer", fontWeight: "bold"
                        }}>DATA EXPLORER</button>
                    </div>

                    <div style={{
                        background: "#141e24", border: "1px solid #1a2e38", borderRadius: 12, padding: "24px"
                    }}>
                        {view === "distribution" ? (
                            <svg ref={svgRef} style={{width: "100%", height: "auto", display: "block"}}/>
                        ) : (
                            <div>
                                <div style={{display: "flex", justifyContent: "space-between", marginBottom: 20}}>
                                    <div style={{display: "flex", gap: 8}}>
                                        {["all", "false_positive", "investigate", "legitimate"].map(f => (
                                            <button key={f} onClick={() => setActiveFilter(f as any)} style={{
                                                background: activeFilter === f ? (COLORS[f as Classification] || "#2a4a58") + "22" : "transparent",
                                                border: `1px solid ${activeFilter === f ? (COLORS[f as Classification] || "#2a4a58") : "#1a2a34"}`,
                                                borderRadius: 20, padding: "5px 14px", color: activeFilter === f ? (COLORS[f as Classification] || "#d0dce0") : "#5a7a84",
                                                fontSize: 11, cursor: "pointer", textTransform: "capitalize"
                                            }}>
                                                {f.replace('_', ' ')}
                                            </button>
                                        ))}
                                    </div>
                                    <input 
                                        type="text" placeholder="Search..." 
                                        value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)}
                                        style={{
                                            background: "#0c1518", border: "1px solid #2a4a58",
                                            borderRadius: 6, padding: "8px 14px", color: "#e8f0f2", width: 200
                                        }}
                                    />
                                </div>
                                
                                <div style={{maxHeight: "500px", overflowY: "auto"}}>
                                    <table style={{width: "100%", borderCollapse: "collapse", fontSize: 12}}>
                                        <thead>
                                            <tr style={{textAlign: "left", color: "#5a7a84", borderBottom: "1px solid #1a2e38"}}>
                                                <th style={{padding: "10px"}}>SKILL</th>
                                                <th style={{padding: "10px"}}>COUNT</th>
                                                <th style={{padding: "10px"}}>LOCAL σ</th>
                                                <th style={{padding: "10px"}}>STATUS</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {filtered.map(s => (
                                                <tr key={s.skill} style={{borderBottom: "1px solid rgba(255,255,255,0.03)"}}>
                                                    <td style={{padding: "10px"}}>
                                                        <div style={{fontWeight: "bold", color: "#e0ecf0"}}>{s.skill}</div>
                                                        <div style={{fontSize: "9px", color: "#4a6a74"}}>{s.group}</div>
                                                    </td>
                                                    <td style={{padding: "10px"}}>{s.count}</td>
                                                    <td style={{padding: "10px"}}>
                                                        <div style={{display: "flex", alignItems: "center", gap: 6}}>
                                                            <span style={{color: s.classification === 'false_positive' ? "#ff4d6a" : s.classification === 'investigate' ? "#f0a030" : "#40d89b", fontWeight: "bold", width: 40}}>
                                                                {((s.log_count - mean) / stdev).toFixed(2)}
                                                            </span>
                                                        </div>
                                                    </td>
                                                    <td style={{padding: "10px"}}>
                                                        <span style={{
                                                            fontSize: "9px", padding: "2px 6px", borderRadius: 3,
                                                            background: COLORS[s.classification] + "22", color: COLORS[s.classification],
                                                            border: `1px solid ${COLORS[s.classification]}44`, fontWeight: "bold"
                                                        }}>{LABELS[s.classification]}</span>
                                                    </td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            </div>
                        )}
                    </div>
                </div>

                {/* Sidebar */}
                <div style={{width: 300}}>
                    <div style={{
                        background: "#141e24", border: "1px solid #1a2e38", borderRadius: 10, padding: "20px",
                        position: "sticky", top: "32px"
                    }}>
                        <h3 style={{fontSize: 13, margin: "0 0 16px 0", color: "#8a9aa4"}}>BIN INSPECTOR</h3>
                        {selectedBin ? (
                            <div>
                                <div style={{marginBottom: 16}}>
                                    <div style={{fontSize: 10, color: "#4a6a74"}}>RANGE [LN(COUNT+1)]</div>
                                    <div style={{fontSize: 14, fontWeight: "bold", color: "#f0a030"}}>
                                        {selectedBin.range[0].toFixed(2)} — {selectedBin.range[1].toFixed(2)}
                                    </div>
                                    <div style={{fontSize: 10, color: "#4a6a74", marginTop: 4}}>
                                        RAW COUNT RANGE: {(Math.exp(selectedBin.range[0]) - 1).toFixed(0)} - {(Math.exp(selectedBin.range[1]) - 1).toFixed(0)}
                                    </div>
                                </div>
                                <div style={{fontSize: 10, color: "#4a6a74", marginBottom: 8, marginTop: 12}}>
                                    SKILLS IN THIS FREQUENCY ({selectedBin.skills.length})
                                </div>
                                <div style={{
                                    maxHeight: "400px", overflowY: "auto", background: "#0c1518",
                                    padding: "12px", borderRadius: 6, border: "1px solid #1a2e38"
                                }}>
                                    {selectedBin.skills.map(s => (
                                        <div key={s.skill} style={{
                                            fontSize: 11, padding: "4px 0", borderBottom: "1px solid #141e24",
                                            color: "#d0dce0", display: "flex", justifyContent: "space-between"
                                        }}>
                                            <span>{s.skill}</span>
                                            <span style={{ color: "#4a6a74", fontSize: "10px" }}>{s.count}</span>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        ) : (
                            <div style={{textAlign: "center", padding: "40px 0"}}>
                                <div style={{fontSize: 40, opacity: 0.1, marginBottom: 12}}>🖱️</div>
                                <div style={{fontSize: 11, color: "#4a6a74"}}>Click any bar to inspect skills.</div>
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}

export default SkillOccurrenceAnomalyDetection;
