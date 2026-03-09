import json
import math
import statistics
import os
from collections import defaultdict

def analyze_anomalies(file_path, output_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    nodes = data.get('nodes', {})
    
    # 1. Load ALL nodes
    all_skills = []
    group_data = defaultdict(list)
    
    for skill, info in nodes.items():
        count = info.get('total_count', 0)
        # Using ln(count + 1) to handle zeros mathematically
        log_c = math.log(count + 1)
        
        skill_obj = {
            'skill': skill,
            'count': count,
            'log_count': log_c,
            'group': info.get('group', 'Unknown'),
            'super_group': info.get('super_group', 'Unknown')
        }
        all_skills.append(skill_obj)
        group_data[skill_obj['group']].append(log_c)
    
    if not all_skills:
        print("No nodes found.")
        return

    # 2. Global Gaussian Fit (including zeros now)
    all_log_counts = [s['log_count'] for s in all_skills]
    global_mean = statistics.mean(all_log_counts)
    global_stdev = statistics.stdev(all_log_counts)
    
    # 3. Per-Group Statistics
    group_stats = {}
    for group, logs in group_data.items():
        if len(logs) > 1:
            group_stats[group] = {
                'mean': statistics.mean(logs),
                'stdev': statistics.stdev(logs)
            }
        else:
            group_stats[group] = {
                'mean': logs[0],
                'stdev': 0.1 # Small epsilon for single items
            }

    # 4. Calculate Z-scores
    for s in all_skills:
        # Global Z-score
        s['z_score'] = (s['log_count'] - global_mean) / global_stdev
        
        # Group Z-score
        stats = group_stats.get(s['group'])
        if stats and stats['stdev'] > 0:
            s['group_z_score'] = (s['log_count'] - stats['mean']) / stats['stdev']
        else:
            s['group_z_score'] = 0.0
            
    # Sort by count descending
    all_skills.sort(key=lambda x: x['count'], reverse=True)
    
    # Save report
    report = {
        "summary": {
            "total_nodes": len(nodes),
            "connected_nodes": len([s for s in all_skills if s['count'] > 0]),
            "zero_count_nodes": len([s for s in all_skills if s['count'] == 0]),
            "log_mean": global_mean,
            "log_stdev": global_stdev,
            "sigma_threshold": 2.0,
            "transform": "ln(count + 1)"
        },
        "all_skills": all_skills
    }
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Backup previous report if it exists
    if os.path.exists(output_path):
        backup_path = output_path.replace('.json', '_previous.json')
        import shutil
        shutil.copy2(output_path, backup_path)
        print(f"Backed up previous report to {backup_path}")

    with open(output_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"Comprehensive report generated with {len(all_skills)} nodes.")
    print(f"Includes {report['summary']['zero_count_nodes']} zero-occurrence skills.")
    print(f"New Global Mean: {global_mean:.4f}, New Global Stdev: {global_stdev:.4f}")

if __name__ == "__main__":
    analyze_anomalies(
        'GlobalInput/Taxonomy/universe.json', 
        'src/dashboards/SkillOccurrenceAnomalyDetection/data/anomaly_report.json'
    )
