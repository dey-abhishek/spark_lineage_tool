"""HTML report generator."""

from pathlib import Path
from typing import Dict, Any

from lineage.lineage import LineageGraph


class HTMLExporter:
    """Generate HTML report for lineage."""
    
    def export(
        self,
        graph: LineageGraph,
        metrics: Dict[str, Any],
        output_path: Path
    ) -> None:
        """Generate HTML report."""
        html = self._generate_html(graph, metrics)
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(html)
        
        print(f"Exported HTML report to {output_path}")
    
    def _generate_html(self, graph: LineageGraph, metrics: Dict[str, Any]) -> str:
        """Generate HTML content."""
        stats = graph.get_stats()
        
        top_nodes = sorted(
            [(node_id, m) for node_id, m in metrics.items()],
            key=lambda x: x[1].priority_score,
            reverse=True
        )[:10]
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Lineage Analysis Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #333; }}
        .stats {{ background: #f0f0f0; padding: 15px; border-radius: 5px; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        .wave-1 {{ background-color: #ffebee; }}
        .wave-2 {{ background-color: #fff9c4; }}
        .wave-3 {{ background-color: #e8f5e9; }}
    </style>
</head>
<body>
    <h1>Spark Lineage Analysis Report</h1>
    
    <div class="stats">
        <h2>Summary Statistics</h2>
        <p><strong>Total Nodes:</strong> {stats['total_nodes']}</p>
        <p><strong>Total Edges:</strong> {stats['total_edges']}</p>
        <p><strong>Dataset Nodes:</strong> {stats['dataset_nodes']}</p>
        <p><strong>Job Nodes:</strong> {stats['job_nodes']}</p>
        <p><strong>Module Nodes:</strong> {stats['module_nodes']}</p>
    </div>
    
    <h2>Top Priority Datasets</h2>
    <table>
        <tr>
            <th>Dataset</th>
            <th>Priority Score</th>
            <th>Downstream Reach</th>
            <th>Fan Out</th>
            <th>Migration Wave</th>
            <th>Confidence</th>
        </tr>
"""
        
        for node_id, m in top_nodes:
            node = graph.get_node_by_id(node_id)
            if node:
                wave_class = f"wave-{m.migration_wave}"
                html += f"""
        <tr class="{wave_class}">
            <td>{node.name}</td>
            <td>{m.priority_score:.2f}</td>
            <td>{m.downstream_reach}</td>
            <td>{m.fan_out}</td>
            <td>Wave {m.migration_wave}</td>
            <td>{m.avg_confidence:.2f}</td>
        </tr>
"""
        
        html += """
    </table>
    
    <h2>Migration Waves</h2>
    <p><span class="wave-1">■</span> Wave 1: High priority, high confidence</p>
    <p><span class="wave-2">■</span> Wave 2: Medium priority</p>
    <p><span class="wave-3">■</span> Wave 3: Low priority or shell-heavy</p>
</body>
</html>
"""
        
        return html

