"""CSV exporter for lineage data."""

from pathlib import Path
import csv
from typing import Dict, Any

from lineage.lineage import LineageGraph


class CSVExporter:
    """Export lineage graph to CSV files."""
    
    def export(
        self,
        graph: LineageGraph,
        metrics: Dict[str, Any],
        output_dir: Path
    ) -> None:
        """Export graph to CSV files."""
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export nodes
        self._export_nodes(graph, output_dir / "nodes.csv")
        
        # Export edges
        self._export_edges(graph, output_dir / "edges.csv")
        
        # Export metrics
        self._export_metrics(metrics, output_dir / "metrics.csv")
        
        print(f"Exported lineage to {output_dir}")
    
    def _export_nodes(self, graph: LineageGraph, output_path: Path) -> None:
        """Export nodes to CSV."""
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["node_id", "node_type", "urn", "name", "metadata"]
            )
            writer.writeheader()
            
            for node in graph.nodes.values():
                writer.writerow({
                    "node_id": node.node_id,
                    "node_type": node.node_type.value,
                    "urn": node.urn,
                    "name": node.name,
                    "metadata": str(node.metadata)
                })
    
    def _export_edges(self, graph: LineageGraph, output_path: Path) -> None:
        """Export edges to CSV."""
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "edge_id", "source_node_id", "target_node_id",
                    "edge_type", "confidence", "evidence"
                ]
            )
            writer.writeheader()
            
            for edge in graph.edges:
                writer.writerow({
                    "edge_id": edge.edge_id,
                    "source_node_id": edge.source_node_id,
                    "target_node_id": edge.target_node_id,
                    "edge_type": edge.edge_type.value,
                    "confidence": edge.confidence,
                    "evidence": edge.evidence[:100]
                })
    
    def _export_metrics(self, metrics: Dict[str, Any], output_path: Path) -> None:
        """Export metrics to CSV."""
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "node_id", "fan_in", "fan_out", "downstream_reach",
                    "priority_score", "migration_wave", "avg_confidence"
                ]
            )
            writer.writeheader()
            
            for node_id, m in metrics.items():
                writer.writerow({
                    "node_id": node_id,
                    "fan_in": m.fan_in,
                    "fan_out": m.fan_out,
                    "downstream_reach": m.downstream_reach,
                    "priority_score": m.priority_score,
                    "migration_wave": m.migration_wave,
                    "avg_confidence": m.avg_confidence
                })

