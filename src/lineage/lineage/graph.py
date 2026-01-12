"""Lineage graph operations."""

from typing import Dict, List, Set, Optional
import networkx as nx

from .models import Node, Edge, NodeType, EdgeType


class LineageGraph:
    """Graph structure for lineage relationships."""
    
    def __init__(self) -> None:
        self.nodes: Dict[str, Node] = {}
        self.edges: List[Edge] = []
        self.graph = nx.DiGraph()
        self._urn_to_node: Dict[str, str] = {}  # URN -> node_id mapping
    
    def add_node(self, node: Node) -> None:
        """Add a node to the graph."""
        self.nodes[node.node_id] = node
        self.graph.add_node(node.node_id, **node.to_dict())
        
        if node.urn:
            self._urn_to_node[node.urn] = node.node_id
    
    def add_edge(self, edge: Edge) -> None:
        """Add an edge to the graph."""
        self.edges.append(edge)
        self.graph.add_edge(
            edge.source_node_id,
            edge.target_node_id,
            **edge.to_dict()
        )
    
    def get_node_by_urn(self, urn: str) -> Optional[Node]:
        """Get node by URN."""
        node_id = self._urn_to_node.get(urn)
        return self.nodes.get(node_id) if node_id else None
    
    def get_node_by_id(self, node_id: str) -> Optional[Node]:
        """Get node by ID."""
        return self.nodes.get(node_id)
    
    def get_upstream_nodes(self, node_id: str) -> List[Node]:
        """Get all upstream nodes."""
        if node_id not in self.graph:
            return []
        
        upstream_ids = list(self.graph.predecessors(node_id))
        return [self.nodes[nid] for nid in upstream_ids if nid in self.nodes]
    
    def get_downstream_nodes(self, node_id: str) -> List[Node]:
        """Get all downstream nodes."""
        if node_id not in self.graph:
            return []
        
        downstream_ids = list(self.graph.successors(node_id))
        return [self.nodes[nid] for nid in downstream_ids if nid in self.nodes]
    
    def get_transitive_downstream(self, node_id: str) -> Set[str]:
        """Get all transitively reachable downstream nodes."""
        if node_id not in self.graph:
            return set()
        
        return set(nx.descendants(self.graph, node_id))
    
    def get_transitive_upstream(self, node_id: str) -> Set[str]:
        """Get all transitively reachable upstream nodes."""
        if node_id not in self.graph:
            return set()
        
        return set(nx.ancestors(self.graph, node_id))
    
    def get_fan_out(self, node_id: str) -> int:
        """Get fan-out count (number of direct downstream nodes)."""
        return self.graph.out_degree(node_id) if node_id in self.graph else 0
    
    def get_fan_in(self, node_id: str) -> int:
        """Get fan-in count (number of direct upstream nodes)."""
        return self.graph.in_degree(node_id) if node_id in self.graph else 0
    
    def get_downstream_reach(self, node_id: str) -> int:
        """Get total downstream reach (transitive)."""
        return len(self.get_transitive_downstream(node_id))
    
    def get_nodes_by_type(self, node_type: NodeType) -> List[Node]:
        """Get all nodes of a specific type."""
        return [node for node in self.nodes.values() if node.node_type == node_type]
    
    def get_stats(self) -> Dict[str, int]:
        """Get graph statistics."""
        return {
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "dataset_nodes": len(self.get_nodes_by_type(NodeType.DATASET)),
            "job_nodes": len(self.get_nodes_by_type(NodeType.JOB)),
            "module_nodes": len(self.get_nodes_by_type(NodeType.MODULE)),
        }

