import json
import math
import statistics

def analyze_anomalies(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    nodes = data.get('nodes', {})
    
    # 1. Load counts and filter for connected nodes (count > 0)
    connected_skills = []
    for skill, info in nodes.items():
        count = info.get('total_count', 0)
        if count > 0:
            connected_skills.append({
                'skill': skill,
                'count': count,
                'group': info.get('group'),
                'super_group': info.get('super_group')
            })
    
    if not connected_skills:
        print("No connected nodes found.")
        return

    # 2. Log-transform the counts
    log_counts = [math.log(s['count']) for s in connected_skills]
    
    # 3. Fit Gaussian (Calculate mean and stdev of log-counts)
    mean = statistics.mean(log_counts)
    stdev = statistics.stdev(log_counts)
    threshold = mean + 2 * stdev
    
    print(f"Summary Statistics (Log-Counts):")
    print(f"Total Nodes: {len(nodes)}")
    print(f"Connected Nodes: {len(connected_skills)}")
    print(f"Mean: {mean:.4f}")
    print(f"Std Dev: {stdev:.4f}")
    print(f"2σ Threshold: {threshold:.4f}")
    print("-" * 40)

    # 4. Flag anything beyond 2σ
    flagged = []
    for s, lc in zip(connected_skills, log_counts):
        if lc > threshold:
            z_score = (lc - mean) / stdev
            s['log_count'] = lc
            s['z_score'] = z_score
            flagged.append(s)
            
    # 5. Output a ranked review list (ranked by Z-score)
    flagged.sort(key=lambda x: x['z_score'], reverse=True)
    
    print(f"Ranked Review List (Flagged Skills > 2σ):")
    print(f"{'Skill':<30} | {'Count':<10} | {'Z-Score':<8} | {'Group'}")
    print("-" * 80)
    for s in flagged:
        print(f"{s['skill']:<30} | {s['count']:<10} | {s['z_score']:>7.2f}σ | {s['group']}")

    # Save summary for potential use by other tools
    summary = {
        "summary": {
            "total_nodes": len(nodes),
            "connected_nodes": len(connected_skills),
            "log_mean": mean,
            "log_stdev": stdev,
            "sigma_threshold": 2.0,
            "flagged_count": len(flagged)
        },
        "flagged_skills": flagged
    }
    with open('src/SkillOccurrenceAnomalyDetection/data/anomaly_report.json', 'w') as f:
        json.dump(summary, f, indent=2)

if __name__ == "__main__":
    analyze_anomalies('GlobalInput/Taxonomy/universe.json')
