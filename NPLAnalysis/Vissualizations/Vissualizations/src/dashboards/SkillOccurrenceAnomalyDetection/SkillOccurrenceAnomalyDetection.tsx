import { useState, useMemo, useRef, useEffect } from "react";
import * as d3 from "d3";
import RAW_DATA_CURRENT from './data/anomaly_report.json'
import RAW_DATA_PREVIOUS from './data/anomaly_report_previous.json'

type Classification = "false_positive" | "investigate" | "legitimate";
type ViewMode = "current" | "previous" | "diff";

interface Skill {
    skill: string;
    count: number;
    log_count: number;
    z_score: number;
    group_z_score: number;
    group: string;
    super_group: string;
    classification: Classification;
    localZ: number;
}

interface DiffSkill extends Skill {
    prev_count: number;
    prev_z_score: number;
    count_delta: number;
    z_delta: number;
    change_type: "new" | "removed" | "increased" | "decreased" | "stable";
}

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
    const [viewMode, setViewMode] = useState<ViewMode>("current");
    const [view, setView] = useState("distribution");
    const [activeFilter, setActiveFilter] = useState<Classification | "all">("all");
    const [sigmaThreshold, setSigmaThreshold] = useState(2.0);
    const [minWeight, setMinWeight] = useState(0); 
    const [searchTerm, setSearchTerm] = useState("");
    const [selectedBin, setSelectedBin] = useState<{skills: {skill: string, count: number}[], range: [number, number]} | null>(null);
    
    const svgRef = useRef<SVGSVGElement>(null);
    const tooltipRef = useRef<HTMLDivElement>(null);

    // Derived properties based on filtered weight and view mode
    const { skills, mean, stdev, totalFilteredNodes, previousSkillsMap } = useMemo(() => {
        const data = viewMode === "previous" ? RAW_DATA_PREVIOUS : RAW_DATA_CURRENT;
        
        // 1. Process Previous Data for Diff
        const prevWeightFiltered = RAW_DATA_PREVIOUS.all_skills.filter((s: any) => s.count >= minWeight);
        const prevLogs = prevWeightFiltered.map((s: any) => s.log_count);
        const prevM = prevLogs.length > 0 ? d3.mean(prevLogs) || 0 : 0;
        const prevS = prevLogs.length > 1 ? d3.deviation(prevLogs) || 1 : 1;
        
        const prevMap = new Map<string, Skill>();
        prevWeightFiltered.forEach((s: any) => {
            const localZ = (s.log_count - prevM) / prevS;
            let classification: Classification = "legitimate";
            if (localZ >= (sigmaThreshold + 1.0)) classification = "false_positive";
            else if (localZ >= sigmaThreshold || s.group_z_score >= 2.5) classification = "investigate";
            prevMap.set(s.skill, { ...s, classification, localZ });
        });

        // 2. Process Current/Selected Data
        const weightFiltered = data.all_skills.filter((s: any) => s.count >= minWeight);
        const logs = weightFiltered.map((s: any) => s.log_count);
        const m = logs.length > 0 ? d3.mean(logs) || 0 : 0;
        const s_dev = logs.length > 1 ? d3.deviation(logs) || 1 : 1;

        const upperMult = sigmaThreshold + 1.0;
        const finalSkills: Skill[] = weightFiltered.map((s: any) => {
            let classification: Classification = "legitimate";
            const localZ = (s.log_count - m) / s_dev;

            if (localZ >= upperMult) {
                classification = "false_positive";
            } else if (localZ >= sigmaThreshold || s.group_z_score >= 2.5) {
                classification = "investigate";
            }
            return { ...s, classification, localZ };
        }).sort((a: Skill, b: Skill) => b.count - a.count);

        return {
            skills: finalSkills,
            mean: m,
            stdev: s_dev,
            totalFilteredNodes: finalSkills.length,
            previousSkillsMap: prevMap
        };
    }, [viewMode, minWeight, sigmaThreshold]);

    const upperThresholdMultiplier = sigmaThreshold + 1.0;

    const diffResults = useMemo(() => {
        if (viewMode !== "diff") return [];
        
        const currentMap = new Map<string, Skill>();
        skills.forEach(s => currentMap.set(s.skill, s));

        const allSkillNames = new Set([...Array.from(currentMap.keys()), ...Array.from(previousSkillsMap.keys())]);
        
        const results: DiffSkill[] = [];
        allSkillNames.forEach(name => {
            const cur = currentMap.get(name);
            const prev = previousSkillsMap.get(name);

            if (cur && prev) {
                const z_delta = cur.localZ - prev.localZ;
                const count_delta = cur.count - prev.count;
                let change_type: DiffSkill["change_type"] = "stable";
                if (z_delta > 0.3) change_type = "increased";
                else if (z_delta < -0.3) change_type = "decreased";

                results.push({
                    ...cur,
                    prev_count: prev.count,
                    prev_z_score: prev.localZ,
                    count_delta,
                    z_delta,
                    change_type
                });
            } else if (cur) {
                results.push({
                    ...cur,
                    prev_count: 0,
                    prev_z_score: 0,
                    count_delta: cur.count,
                    z_delta: cur.localZ,
                    change_type: "new"
                });
            } else if (prev) {
                results.push({
                    ...prev,
                    prev_count: prev.count,
                    prev_z_score: prev.localZ,
                    count_delta: -prev.count,
                    z_delta: -prev.localZ,
                    change_type: "removed"
                });
            }
        });

        return results.sort((a, b) => Math.abs(b.z_delta) - Math.abs(a.z_delta));
    }, [viewMode, skills, previousSkillsMap]);

    const filtered = useMemo(() => {
        let result = viewMode === "diff" ? diffResults : skills;
        if (activeFilter !== "all") {
            result = result.filter(s => s.classification === activeFilter);
        }
        if (searchTerm) {
            result = result.filter(s => s.skill.toLowerCase().includes(searchTerm.toLowerCase()));
        }
        return result;
    }, [skills, diffResults, viewMode, activeFilter, searchTerm]);

    const counts = useMemo(() => {
        const c: Record<Classification, number> = {false_positive: 0, investigate: 0, legitimate: 0};
        skills.forEach((s: Skill) => {
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

        // Previous Mean Line (for comparison if in Current view)
        if (viewMode === "current" && RAW_DATA_PREVIOUS) {
            // Re-calculating previous mean roughly
            const prevMean = 1.354; // Roughly known from earlier
            const prevX = x(prevMean);
            if (prevX >= 0 && prevX <= width) {
                g.append("line")
                    .attr("x1", prevX).attr("x2", prevX)
                    .attr("y1", 0).attr("y2", height)
                    .attr("stroke", "rgba(255,255,255,0.2)")
                    .attr("stroke-dasharray", "2,2");
            }
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

    }, [view, mean, stdev, sigmaThreshold, upperThresholdMultiplier, skills, minWeight, viewMode]);

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

            {/* Mode Switcher */}
            <div style={{display: "flex", gap: 12, marginBottom: 24}}>
                {(["current", "previous", "diff"] as ViewMode[]).map(mode => (
                    <button key={mode} onClick={() => setViewMode(mode)} style={{
                        background: viewMode === mode ? "#40d89b22" : "#141e24",
                        border: `1px solid ${viewMode === mode ? "#40d89b" : "#1a2e38"}`,
                        borderRadius: 6, padding: "8px 20px", color: viewMode === mode ? "#40d89b" : "#8a9aa4",
                        fontSize: 12, cursor: "pointer", fontWeight: "bold", textTransform: "uppercase"
                    }}>
                        {mode === 'diff' ? '⚡ Diff Analysis' : mode + ' Run'}
                    </button>
                ))}
            </div>

            {/* Header Area */}
            <div style={{display: "flex", justifyContent: "space-between", marginBottom: 32, gap: 24}}>
                <div style={{flex: 1}}>
                    <h1 style={{fontSize: 22, fontWeight: 700, color: "#e8f0f2", margin: 0}}>Skill Anomaly Analytics</h1>
                    <p style={{fontSize: 13, color: "#6a8a94", marginTop: 8}}>
                        Dataset: {totalFilteredNodes.toLocaleString()} skills (Filtered at weight ≥ {minWeight}) 
                        {viewMode === 'diff' && <span style={{color: "#f0a030", marginLeft: 8}}>| Mode: Comparison</span>}
                    </p>
                </div>

                <div style={{
                    background: "#141e24", padding: "20px", borderRadius: 10, border: "1px solid #1a2e38", width: 420,
                    display: "flex", flexDirection: "column", gap: "20px"
                }}>
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
                                                <th style={{padding: "10px"}}>COUNT {viewMode === 'diff' && '(Δ)'}</th>
                                                <th style={{padding: "10px"}}>LOCAL σ {viewMode === 'diff' && '(Δ)'}</th>
                                                <th style={{padding: "10px"}}>STATUS</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {filtered.map((s: any) => (
                                                <tr key={s.skill} style={{borderBottom: "1px solid rgba(255,255,255,0.03)"}}>
                                                    <td style={{padding: "10px"}}>
                                                        <div style={{fontWeight: "bold", color: "#e0ecf0"}}>{s.skill}</div>
                                                        <div style={{fontSize: "9px", color: "#4a6a74"}}>{s.group}</div>
                                                    </td>
                                                    <td style={{padding: "10px"}}>
                                                        {s.count} 
                                                        {viewMode === 'diff' && (
                                                            <span style={{color: s.count_delta > 0 ? "#40d89b" : s.count_delta < 0 ? "#ff4d6a" : "#4a6a74", marginLeft: 6, fontSize: "10px"}}>
                                                                ({s.count_delta > 0 ? '+' : ''}{s.count_delta})
                                                            </span>
                                                        )}
                                                    </td>
                                                    <td style={{padding: "10px"}}>
                                                        <div style={{display: "flex", alignItems: "center", gap: 6}}>
                                                            <span style={{color: s.classification === 'false_positive' ? "#ff4d6a" : s.classification === 'investigate' ? "#f0a030" : "#40d89b", fontWeight: "bold", width: 40}}>
                                                                {((s.log_count - mean) / stdev).toFixed(2)}
                                                            </span>
                                                            {viewMode === 'diff' && (
                                                                <span style={{color: s.z_delta > 0.1 ? "#ff4d6a" : s.z_delta < -0.1 ? "#40d89b" : "#4a6a74", fontSize: "10px"}}>
                                                                    ({s.z_delta > 0 ? '+' : ''}{s.z_delta.toFixed(2)})
                                                                </span>
                                                            )}
                                                        </div>
                                                    </td>
                                                    <td style={{padding: "10px"}}>
                                                        {viewMode === 'diff' ? (
                                                            <span style={{
                                                                fontSize: "9px", padding: "2px 6px", borderRadius: 3,
                                                                background: (s.change_type === 'new' ? "#40d89b" : s.change_type === 'increased' ? "#ff4d6a" : "#1a2e38") + "22",
                                                                color: s.change_type === 'new' ? "#40d89b" : s.change_type === 'increased' ? "#ff4d6a" : "#8a9aa4",
                                                                border: `1px solid ${s.change_type === 'new' ? "#40d89b" : s.change_type === 'increased' ? "#ff4d6a" : "#1a2e38"}44`,
                                                                fontWeight: "bold"
                                                            }}>{s.change_type.toUpperCase()}</span>
                                                        ) : (
                                                            <span style={{
                                                                fontSize: "9px", padding: "2px 6px", borderRadius: 3,
                                                                background: COLORS[s.classification as Classification] + "22", color: COLORS[s.classification as Classification],
                                                                border: `1px solid ${COLORS[s.classification as Classification]}44`, fontWeight: "bold"
                                                            }}>{LABELS[s.classification as Classification]}</span>
                                                        )}
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

                <div style={{width: 300}}>
                    <div style={{
                        background: "#141e24", border: "1px solid #1a2e38", borderRadius: 10, padding: "20px",
                        position: "sticky", top: "32px"
                    }}>
                        <h3 style={{fontSize: 13, margin: "0 0 16px 0", color: "#8a9aa4"}}>INSIGHTS {viewMode === 'diff' && ' (DIFF)'}</h3>
                        
                        {viewMode === 'diff' ? (
                            <div>
                                <div style={{marginBottom: 20}}>
                                    <div style={{fontSize: 10, color: "#4a6a74", marginBottom: 4}}>NEWLY ADDED SKILLS</div>
                                    <div style={{fontSize: 18, fontWeight: "bold", color: "#40d89b"}}>
                                        {diffResults.filter(r => r.change_type === 'new').length}
                                    </div>
                                </div>
                                <div style={{marginBottom: 20}}>
                                    <div style={{fontSize: 10, color: "#4a6a74", marginBottom: 4}}>SIGNIFICANT SHIFTS</div>
                                    <div style={{fontSize: 18, fontWeight: "bold", color: "#f0a030"}}>
                                        {diffResults.filter(r => r.change_type === 'increased' || r.change_type === 'decreased').length}
                                    </div>
                                </div>
                                <div style={{fontSize: 10, color: "#4a6a74", marginBottom: 8}}>TOP SHIFTERS (BY σ)</div>
                                <div style={{maxHeight: "300px", overflowY: "auto"}}>
                                    {diffResults.slice(0, 10).map(s => (
                                        <div key={s.skill} style={{fontSize: 11, padding: "6px 0", borderBottom: "1px solid #141e24"}}>
                                            <div style={{display: "flex", justifyContent: "space-between"}}>
                                                <span style={{color: "#e8f0f2"}}>{s.skill}</span>
                                                <span style={{color: s.z_delta > 0 ? "#ff4d6a" : "#40d89b"}}>Δ {s.z_delta.toFixed(2)}σ</span>
                                            </div>
                                            <div style={{fontSize: 9, color: "#4a6a74"}}>{s.prev_count} → {s.count} counts</div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        ) : (
                            selectedBin ? (
                                <div>
                                    <div style={{marginBottom: 16}}>
                                        <div style={{fontSize: 10, color: "#4a6a74"}}>RANGE [LN(COUNT+1)]</div>
                                        <div style={{fontSize: 14, fontWeight: "bold", color: "#f0a030"}}>
                                            {selectedBin.range[0].toFixed(2)} — {selectedBin.range[1].toFixed(2)}
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
                            )
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}

export default SkillOccurrenceAnomalyDetection;
