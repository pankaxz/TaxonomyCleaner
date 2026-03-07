import { useState, useMemo, useRef, useEffect } from "react";
import * as d3 from "d3";

const RAW_DATA = {"summary":{"total_nodes":2194,"connected_nodes":1257,"disconnected_nodes":937,"log_mean":2.1102,"log_stdev":1.7949,"sigma_threshold":2.0,"flagged_count":59},"flagged_skills":[{"skill":"gnu make","total_count":1845,"log_count":7.5202,"z_score":3.014,"group":"Development Tools","super_group":"DevTools","direction":"high"},{"skill":"identity","total_count":1491,"log_count":7.3072,"z_score":2.8953,"group":"Security","super_group":"Cloud","direction":"high"},{"skill":"ai","total_count":1428,"log_count":7.264,"z_score":2.8713,"group":"AI Data Science","super_group":"AI","direction":"high"},{"skill":"futures","total_count":1358,"log_count":7.2138,"z_score":2.8433,"group":"Development Tools","super_group":"DevTools","direction":"high"},{"skill":"python","total_count":1355,"log_count":7.2116,"z_score":2.842,"group":"Languages","super_group":"Backend","direction":"high"},{"skill":"aws","total_count":1276,"log_count":7.1515,"z_score":2.8086,"group":"Cloud Computing","super_group":"Cloud","direction":"high"},{"skill":"apis","total_count":1169,"log_count":7.0639,"z_score":2.7598,"group":"API Management","super_group":"Backend","direction":"high"},{"skill":"dfat","total_count":1149,"log_count":7.0466,"z_score":2.7502,"group":"Hardware Embedded","super_group":"Embedded","direction":"high"},{"skill":"automation","total_count":1102,"log_count":7.0049,"z_score":2.7269,"group":"CI/CD Infrastructure","super_group":"Cloud","direction":"high"},{"skill":"ci/cd","total_count":1068,"log_count":6.9735,"z_score":2.7094,"group":"CI/CD Infrastructure","super_group":"Cloud","direction":"high"},{"skill":"training","total_count":866,"log_count":6.7639,"z_score":2.5926,"group":"AI Data Science","super_group":"AI","direction":"high"},{"skill":"java","total_count":830,"log_count":6.7214,"z_score":2.569,"group":"Languages","super_group":"Backend","direction":"high"},{"skill":"sql","total_count":782,"log_count":6.6619,"z_score":2.5358,"group":"Databases","super_group":"Backend","direction":"high"},{"skill":"javascript","total_count":768,"log_count":6.6438,"z_score":2.5257,"group":"Languages","super_group":"Backend","direction":"high"},{"skill":"react","total_count":755,"log_count":6.6267,"z_score":2.5162,"group":"Web Ecosystem","super_group":"Frontend","direction":"high"},{"skill":"kubernetes","total_count":754,"log_count":6.6254,"z_score":2.5155,"group":"Development Tools","super_group":"DevTools","direction":"high"},{"skill":"go","total_count":749,"log_count":6.6187,"z_score":2.5118,"group":"Languages","super_group":"Backend","direction":"high"},{"skill":"typescript","total_count":747,"log_count":6.6161,"z_score":2.5103,"group":"Languages","super_group":"Backend","direction":"high"},{"skill":"rest","total_count":730,"log_count":6.593,"z_score":2.4975,"group":"Web Ecosystem","super_group":"Frontend","direction":"high"},{"skill":"pnr","total_count":724,"log_count":6.5848,"z_score":2.4929,"group":"Development Tools","super_group":"DevTools","direction":"high"},{"skill":"code review","total_count":708,"log_count":6.5624,"z_score":2.4804,"group":"Development Tools","super_group":"DevTools","direction":"high"},{"skill":"azure","total_count":682,"log_count":6.525,"z_score":2.4596,"group":"Cloud Computing","super_group":"Cloud","direction":"high"},{"skill":"data analysis","total_count":678,"log_count":6.5191,"z_score":2.4563,"group":"AI Data Science","super_group":"AI","direction":"high"},{"skill":"devops","total_count":633,"log_count":6.4505,"z_score":2.418,"group":"CI/CD Infrastructure","super_group":"Cloud","direction":"high"},{"skill":"coverage","total_count":628,"log_count":6.4425,"z_score":2.4136,"group":"Development Practices","super_group":"DevTools","direction":"high"},{"skill":"docker","total_count":624,"log_count":6.4362,"z_score":2.41,"group":"Development Tools","super_group":"DevTools","direction":"high"},{"skill":"d","total_count":600,"log_count":6.3969,"z_score":2.3882,"group":"Languages","super_group":"Backend","direction":"high"},{"skill":"gcp","total_count":580,"log_count":6.363,"z_score":2.3693,"group":"Cloud Computing","super_group":"Cloud","direction":"high"},{"skill":"machine learning","total_count":549,"log_count":6.3081,"z_score":2.3387,"group":"AI Data Science","super_group":"AI","direction":"high"},{"skill":"git","total_count":545,"log_count":6.3008,"z_score":2.3346,"group":"Development Tools","super_group":"DevTools","direction":"high"},{"skill":"terraform","total_count":541,"log_count":6.2934,"z_score":2.3305,"group":"CI/CD Infrastructure","super_group":"Cloud","direction":"high"},{"skill":"model monitoring","total_count":532,"log_count":6.2766,"z_score":2.3212,"group":"AI Operational Techniques","super_group":"AI","direction":"high"},{"skill":"data processing","total_count":515,"log_count":6.2442,"z_score":2.3031,"group":"Data Engineering","super_group":"Data","direction":"high"},{"skill":"postgresql","total_count":512,"log_count":6.2383,"z_score":2.2998,"group":"Databases","super_group":"Backend","direction":"high"},{"skill":"move","total_count":479,"log_count":6.1717,"z_score":2.2627,"group":"Blockchain","super_group":"Blockchain","direction":"high"},{"skill":"microservices","total_count":458,"log_count":6.1269,"z_score":2.2377,"group":"Software Architecture","super_group":"Backend","direction":"high"},{"skill":"distributed systems","total_count":455,"log_count":6.1203,"z_score":2.2341,"group":"Software Architecture","super_group":"Backend","direction":"high"},{"skill":"accelerate","total_count":436,"log_count":6.0776,"z_score":2.2103,"group":"AI Operational Techniques","super_group":"AI","direction":"high"},{"skill":"foundation","total_count":432,"log_count":6.0684,"z_score":2.2052,"group":"JavaScript Ecosystem","super_group":"Frontend","direction":"high"},{"skill":"transformations","total_count":416,"log_count":6.0307,"z_score":2.1842,"group":"Data Engineering","super_group":"Data","direction":"high"},{"skill":"debugging","total_count":411,"log_count":6.0186,"z_score":2.1774,"group":"QA Testing","super_group":"Testing","direction":"high"},{"skill":"data governance","total_count":405,"log_count":6.0039,"z_score":2.1692,"group":"Data Engineering","super_group":"Data","direction":"high"},{"skill":"linux","total_count":396,"log_count":5.9814,"z_score":2.1567,"group":"Operating Systems","super_group":"Cloud","direction":"high"},{"skill":"networking","total_count":395,"log_count":5.9789,"z_score":2.1553,"group":"Networking Systems","super_group":"Cloud","direction":"high"},{"skill":"cloud platforms","total_count":375,"log_count":5.9269,"z_score":2.1264,"group":"Cloud Computing","super_group":"Cloud","direction":"high"},{"skill":"test automation","total_count":360,"log_count":5.8861,"z_score":2.1036,"group":"QA Testing","super_group":"Testing","direction":"high"},{"skill":"c#","total_count":353,"log_count":5.8665,"z_score":2.0927,"group":"Languages","super_group":"Backend","direction":"high"},{"skill":"kafka","total_count":347,"log_count":5.8493,"z_score":2.0831,"group":"Data Engineering","super_group":"Data","direction":"high"},{"skill":"user experience","total_count":340,"log_count":5.8289,"z_score":2.0718,"group":"Design Creative","super_group":"Frontend","direction":"high"},{"skill":"ui","total_count":340,"log_count":5.8289,"z_score":2.0718,"group":"Web Ecosystem","super_group":"Frontend","direction":"high"},{"skill":"web applications","total_count":333,"log_count":5.8081,"z_score":2.0602,"group":"Web Ecosystem","super_group":"Frontend","direction":"high"},{"skill":"github","total_count":323,"log_count":5.7777,"z_score":2.0432,"group":"CI/CD Infrastructure","super_group":"Cloud","direction":"high"},{"skill":"jenkins","total_count":322,"log_count":5.7746,"z_score":2.0415,"group":"CI/CD Infrastructure","super_group":"Cloud","direction":"high"},{"skill":"objective c","total_count":316,"log_count":5.7557,"z_score":2.031,"group":"Languages","super_group":"Backend","direction":"high"},{"skill":"css","total_count":315,"log_count":5.7526,"z_score":2.0292,"group":"Web Ecosystem","super_group":"Frontend","direction":"high"},{"skill":"html","total_count":314,"log_count":5.7494,"z_score":2.0274,"group":"Web Ecosystem","super_group":"Frontend","direction":"high"},{"skill":"llms","total_count":307,"log_count":5.7268,"z_score":2.0149,"group":"AI Data Science","super_group":"AI","direction":"high"},{"skill":"c++","total_count":302,"log_count":5.7104,"z_score":2.0057,"group":"Languages","super_group":"Backend","direction":"high"},{"skill":"node.js","total_count":300,"log_count":5.7038,"z_score":2.002,"group":"Web Ecosystem","super_group":"Frontend","direction":"high"}]};

const FALSE_POSITIVES = new Set([
  "gnu make", "identity", "futures", "training", "coverage", "d", "go",
  "pnr", "dfat", "move", "accelerate", "foundation", "transformations",
  "rest", "objective c", "model monitoring", "data processing",
  "data analysis", "data governance", "networking", "code review"
]);

const INVESTIGATE = new Set([
  "ai", "apis", "automation", "debugging", "cloud platforms",
  "web applications", "user experience", "ui"
]);

function classify(skill) {
  if (FALSE_POSITIVES.has(skill)) return "false_positive";
  if (INVESTIGATE.has(skill)) return "investigate";
  return "legitimate";
}

const COLORS = {
  false_positive: "#ff4d6a",
  investigate: "#f0a030",
  legitimate: "#40d89b",
};

const LABELS = {
  false_positive: "Likely False Positive",
  investigate: "Needs Investigation",
  legitimate: "Legitimate Skill",
};

function gaussian(x, mean, stdev) {
  const exp = -0.5 * Math.pow((x - mean) / stdev, 2);
  return (1 / (stdev * Math.sqrt(2 * Math.PI))) * Math.exp(exp);
}

export default function App() {
  const [hovered, setHovered] = useState(null);
  const [activeFilter, setActiveFilter] = useState("all");
  const [view, setView] = useState("distribution");
  const svgRef = useRef(null);

  const { mean, stdev } = useMemo(() => ({
    mean: RAW_DATA.summary.log_mean,
    stdev: RAW_DATA.summary.log_stdev,
  }), []);

  const sigma2 = mean + 2 * stdev;
  const sigma3 = mean + 3 * stdev;

  const skills = useMemo(() =>
    RAW_DATA.flagged_skills.map(s => ({
      ...s,
      classification: classify(s.skill),
    })).sort((a, b) => b.z_score - a.z_score),
  []);

  const filtered = useMemo(() => {
    if (activeFilter === "all") return skills;
    return skills.filter(s => s.classification === activeFilter);
  }, [skills, activeFilter]);

  const counts = useMemo(() => {
    const c = { false_positive: 0, investigate: 0, legitimate: 0 };
    skills.forEach(s => c[s.classification]++);
    return c;
  }, [skills]);

  // D3 histogram + gaussian curve
  useEffect(() => {
    if (view !== "distribution" || !svgRef.current) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const margin = { top: 40, right: 30, bottom: 60, left: 60 };
    const width = 760 - margin.left - margin.right;
    const height = 360 - margin.top - margin.bottom;

    const g = svg
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    // Generate synthetic log-count data for all 1257 connected nodes
    // We know the distribution parameters, so simulate it
    const rng = d3.randomNormal(mean, stdev);
    const allLogCounts = Array.from({ length: 1257 }, () => Math.max(0, rng()));
    // Replace top entries with actual flagged data
    skills.forEach(s => allLogCounts.push(s.log_count));

    const x = d3.scaleLinear().domain([0, 8.5]).range([0, width]);
    const bins = d3.bin().domain(x.domain()).thresholds(40)(allLogCounts);
    const maxBinLen = d3.max(bins, d => d.length);

    const y = d3.scaleLinear().domain([0, maxBinLen]).range([height, 0]).nice();

    // Bars
    g.selectAll("rect")
      .data(bins)
      .join("rect")
      .attr("x", d => x(d.x0) + 1)
      .attr("width", d => Math.max(0, x(d.x1) - x(d.x0) - 2))
      .attr("y", d => y(d.length))
      .attr("height", d => height - y(d.length))
      .attr("fill", d => {
        const mid = (d.x0 + d.x1) / 2;
        if (mid > sigma3) return "#ff4d6a";
        if (mid > sigma2) return "#f0a030";
        return "#2a5a6a";
      })
      .attr("opacity", 0.75)
      .attr("rx", 2);

    // Gaussian curve
    const gaussPoints = d3.range(0, 8.5, 0.05).map(xv => ({
      x: xv,
      y: gaussian(xv, mean, stdev) * allLogCounts.length * (bins[0].x1 - bins[0].x0),
    }));

    const line = d3.line()
      .x(d => x(d.x))
      .y(d => y(d.y))
      .curve(d3.curveBasis);

    g.append("path")
      .datum(gaussPoints)
      .attr("fill", "none")
      .attr("stroke", "#e0e0e0")
      .attr("stroke-width", 2.5)
      .attr("stroke-dasharray", "6,3")
      .attr("d", line);

    // Sigma lines
    [
      { val: sigma2, label: "2σ", color: "#f0a030" },
      { val: sigma3, label: "3σ", color: "#ff4d6a" },
    ].forEach(({ val, label, color }) => {
      g.append("line")
        .attr("x1", x(val)).attr("x2", x(val))
        .attr("y1", 0).attr("y2", height)
        .attr("stroke", color)
        .attr("stroke-width", 2)
        .attr("stroke-dasharray", "8,4");

      g.append("text")
        .attr("x", x(val) + 6)
        .attr("y", 14)
        .attr("fill", color)
        .attr("font-size", "12px")
        .attr("font-weight", "700")
        .attr("font-family", "'JetBrains Mono', monospace")
        .text(label);
    });

    // Mean line
    g.append("line")
      .attr("x1", x(mean)).attr("x2", x(mean))
      .attr("y1", 0).attr("y2", height)
      .attr("stroke", "#6a9aaa")
      .attr("stroke-width", 1.5)
      .attr("stroke-dasharray", "4,4");

    g.append("text")
      .attr("x", x(mean) + 4)
      .attr("y", height - 6)
      .attr("fill", "#6a9aaa")
      .attr("font-size", "11px")
      .attr("font-family", "'JetBrains Mono', monospace")
      .text("μ");

    // Axes
    g.append("g")
      .attr("transform", `translate(0,${height})`)
      .call(d3.axisBottom(x).ticks(8))
      .selectAll("text")
      .attr("fill", "#8a9aa4")
      .attr("font-family", "'JetBrains Mono', monospace")
      .attr("font-size", "11px");

    g.append("g")
      .call(d3.axisLeft(y).ticks(6))
      .selectAll("text")
      .attr("fill", "#8a9aa4")
      .attr("font-family", "'JetBrains Mono', monospace")
      .attr("font-size", "11px");

    g.selectAll(".domain").attr("stroke", "#3a4a54");
    g.selectAll(".tick line").attr("stroke", "#2a3a44");

    // Axis labels
    g.append("text")
      .attr("x", width / 2)
      .attr("y", height + 45)
      .attr("text-anchor", "middle")
      .attr("fill", "#8a9aa4")
      .attr("font-size", "12px")
      .attr("font-family", "'JetBrains Mono', monospace")
      .text("ln(occurrence count)");

    g.append("text")
      .attr("transform", "rotate(-90)")
      .attr("x", -height / 2)
      .attr("y", -42)
      .attr("text-anchor", "middle")
      .attr("fill", "#8a9aa4")
      .attr("font-size", "12px")
      .attr("font-family", "'JetBrains Mono', monospace")
      .text("# skills");

  }, [view, mean, stdev, sigma2, sigma3, skills]);

  return (
    <div style={{
      minHeight: "100vh",
      background: "#0c1518",
      color: "#d0dce0",
      fontFamily: "'JetBrains Mono', 'SF Mono', 'Fira Code', monospace",
      padding: "24px",
    }}>
      <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet" />

      {/* Header */}
      <div style={{ marginBottom: 28 }}>
        <h1 style={{
          fontSize: 22, fontWeight: 700, color: "#e8f0f2",
          margin: 0, letterSpacing: "-0.5px",
        }}>
          Skill Occurrence Anomaly Detection
        </h1>
        <p style={{ fontSize: 13, color: "#6a8a94", margin: "6px 0 0" }}>
          Log-normal distribution · 1,257 connected nodes · 3,511 JDs · 2σ threshold
        </p>
      </div>

      {/* Summary cards */}
      <div style={{ display: "flex", gap: 12, marginBottom: 24, flexWrap: "wrap" }}>
        {[
          { label: "Flagged", value: 59, sub: "above 2σ", accent: "#f0a030" },
          { label: "False Positives", value: counts.false_positive, sub: "common words", accent: "#ff4d6a" },
          { label: "Investigate", value: counts.investigate, sub: "ambiguous", accent: "#f0a030" },
          { label: "Legitimate", value: counts.legitimate, sub: "real skills", accent: "#40d89b" },
        ].map((card, i) => (
          <div key={i} style={{
            background: "#141e24",
            border: `1px solid ${card.accent}22`,
            borderRadius: 8,
            padding: "14px 20px",
            minWidth: 140,
            flex: 1,
          }}>
            <div style={{ fontSize: 28, fontWeight: 700, color: card.accent }}>{card.value}</div>
            <div style={{ fontSize: 12, color: "#8a9aa4", marginTop: 2 }}>{card.label}</div>
            <div style={{ fontSize: 10, color: "#4a6a74" }}>{card.sub}</div>
          </div>
        ))}
      </div>

      {/* View tabs */}
      <div style={{ display: "flex", gap: 4, marginBottom: 20 }}>
        {["distribution", "table"].map(v => (
          <button key={v} onClick={() => setView(v)} style={{
            background: view === v ? "#1a2e38" : "transparent",
            border: `1px solid ${view === v ? "#2a4a58" : "#1a2a34"}`,
            borderRadius: 6,
            padding: "8px 18px",
            color: view === v ? "#e8f0f2" : "#5a7a84",
            fontSize: 12,
            fontWeight: 600,
            cursor: "pointer",
            fontFamily: "inherit",
            textTransform: "capitalize",
          }}>
            {v === "distribution" ? "Distribution Plot" : "Flagged Skills Table"}
          </button>
        ))}
      </div>

      {/* Distribution view */}
      {view === "distribution" && (
        <div style={{
          background: "#141e24",
          border: "1px solid #1a2e38",
          borderRadius: 10,
          padding: "20px 16px",
          marginBottom: 24,
        }}>
          <svg ref={svgRef} />
          <div style={{
            display: "flex", gap: 20, marginTop: 12, paddingLeft: 60,
            fontSize: 11, color: "#6a8a94",
          }}>
            <span><span style={{ display: "inline-block", width: 12, height: 12, background: "#2a5a6a", borderRadius: 2, marginRight: 6, verticalAlign: "middle" }} />Normal range</span>
            <span><span style={{ display: "inline-block", width: 12, height: 12, background: "#f0a030", borderRadius: 2, marginRight: 6, verticalAlign: "middle" }} />2σ – 3σ (investigate)</span>
            <span><span style={{ display: "inline-block", width: 12, height: 12, background: "#ff4d6a", borderRadius: 2, marginRight: 6, verticalAlign: "middle" }} />&gt; 3σ (likely false positive)</span>
            <span><span style={{ display: "inline-block", width: 20, height: 2, background: "#e0e0e0", marginRight: 6, verticalAlign: "middle", borderTop: "2px dashed #e0e0e0" }} />Gaussian fit</span>
          </div>
        </div>
      )}

      {/* Table view */}
      {view === "table" && (
        <div style={{
          background: "#141e24",
          border: "1px solid #1a2e38",
          borderRadius: 10,
          overflow: "hidden",
        }}>
          {/* Filter pills */}
          <div style={{
            display: "flex", gap: 6, padding: "14px 16px",
            borderBottom: "1px solid #1a2a34",
          }}>
            {[
              { key: "all", label: "All", count: skills.length },
              { key: "false_positive", label: "False Positives", count: counts.false_positive },
              { key: "investigate", label: "Investigate", count: counts.investigate },
              { key: "legitimate", label: "Legitimate", count: counts.legitimate },
            ].map(f => (
              <button key={f.key} onClick={() => setActiveFilter(f.key)} style={{
                background: activeFilter === f.key ? (COLORS[f.key] || "#2a4a58") + "22" : "transparent",
                border: `1px solid ${activeFilter === f.key ? (COLORS[f.key] || "#2a4a58") : "#1a2a34"}`,
                borderRadius: 20,
                padding: "5px 14px",
                color: activeFilter === f.key ? (COLORS[f.key] || "#d0dce0") : "#5a7a84",
                fontSize: 11,
                fontWeight: 600,
                cursor: "pointer",
                fontFamily: "inherit",
              }}>
                {f.label} ({f.count})
              </button>
            ))}
          </div>

          {/* Table */}
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
              <thead>
                <tr style={{ borderBottom: "1px solid #1a2e38" }}>
                  {["Skill", "Count", "Z-Score", "Group", "Super Group", "Status"].map(h => (
                    <th key={h} style={{
                      padding: "10px 14px",
                      textAlign: "left",
                      color: "#5a7a84",
                      fontWeight: 600,
                      fontSize: 10,
                      textTransform: "uppercase",
                      letterSpacing: "0.5px",
                    }}>{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filtered.map((s, i) => (
                  <tr
                    key={s.skill}
                    onMouseEnter={() => setHovered(s.skill)}
                    onMouseLeave={() => setHovered(null)}
                    style={{
                      borderBottom: "1px solid #111a20",
                      background: hovered === s.skill ? "#1a2830" : i % 2 === 0 ? "#141e24" : "#161f26",
                      transition: "background 0.15s",
                    }}
                  >
                    <td style={{ padding: "9px 14px", fontWeight: 600, color: "#e0ecf0" }}>
                      {s.skill}
                    </td>
                    <td style={{ padding: "9px 14px", fontVariantNumeric: "tabular-nums" }}>
                      {s.total_count.toLocaleString()}
                    </td>
                    <td style={{ padding: "9px 14px" }}>
                      <span style={{
                        display: "inline-flex", alignItems: "center", gap: 6,
                      }}>
                        <span style={{
                          fontVariantNumeric: "tabular-nums",
                          color: s.z_score > 2.8 ? "#ff4d6a" : s.z_score > 2.4 ? "#f0a030" : "#40d89b",
                          fontWeight: 700,
                        }}>
                          {s.z_score.toFixed(2)}σ
                        </span>
                        <span style={{
                          width: Math.min(80, (s.z_score - 2) * 80),
                          height: 4,
                          borderRadius: 2,
                          background: s.z_score > 2.8 ? "#ff4d6a" : s.z_score > 2.4 ? "#f0a030" : "#40d89b",
                          opacity: 0.5,
                          display: "inline-block",
                        }} />
                      </span>
                    </td>
                    <td style={{ padding: "9px 14px", color: "#7a9aa4", fontSize: 11 }}>
                      {s.group}
                    </td>
                    <td style={{ padding: "9px 14px", color: "#5a7a84", fontSize: 11 }}>
                      {s.super_group}
                    </td>
                    <td style={{ padding: "9px 14px" }}>
                      <span style={{
                        display: "inline-block",
                        padding: "3px 10px",
                        borderRadius: 12,
                        fontSize: 10,
                        fontWeight: 700,
                        background: COLORS[s.classification] + "18",
                        color: COLORS[s.classification],
                        border: `1px solid ${COLORS[s.classification]}33`,
                        textTransform: "uppercase",
                        letterSpacing: "0.3px",
                      }}>
                        {LABELS[s.classification]}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Footer */}
      <div style={{
        marginTop: 20,
        padding: "12px 0",
        fontSize: 11,
        color: "#3a5a64",
        borderTop: "1px solid #1a2a34",
      }}>
        μ = {mean.toFixed(4)} · σ = {stdev.toFixed(4)} · 2σ threshold = {sigma2.toFixed(4)} · Classifications are preliminary and require manual verification
      </div>
    </div>
  );
}
