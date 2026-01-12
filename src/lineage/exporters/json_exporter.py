"""JSON exporter for lineage data."""

from pathlib import Path
import json
from typing import Dict, Any

from lineage.lineage import LineageGraph
from lineage.scoring import PriorityCalculator


class JSONExporter:
    """Export lineage graph to JSON format."""
    
    def export(
        self,
        graph: LineageGraph,
        metrics: Dict[str, Any],
        output_path: Path
    ) -> None:
        """Export graph to JSON file."""
        data = {
            "metadata": {
                "total_nodes": len(graph.nodes),
                "total_edges": len(graph.edges),
                **graph.get_stats()
            },
            "nodes": [node.to_dict() for node in graph.nodes.values()],
            "edges": [edge.to_dict() for edge in graph.edges],
            "metrics": {
                node_id: {
                    "fan_in": m.fan_in,
                    "fan_out": m.fan_out,
                    "downstream_reach": m.downstream_reach,
                    "priority_score": m.priority_score,
                    "migration_wave": m.migration_wave,
                    "avg_confidence": m.avg_confidence
                }
                for node_id, m in metrics.items()
            }
        }
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        
        print(f"Exported lineage to {output_path}")

