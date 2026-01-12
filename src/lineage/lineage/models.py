"""Lineage graph data models."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from enum import Enum
from datetime import datetime
import uuid


class NodeType(Enum):
    """Types of nodes in the lineage graph."""
    
    DATASET = "DATASET"
    JOB = "JOB"
    MODULE = "MODULE"


class EdgeType(Enum):
    """Types of edges in the lineage graph."""
    
    READ = "READ"
    WRITE = "WRITE"
    DEPENDS_ON = "DEPENDS_ON"
    PRODUCES = "PRODUCES"


@dataclass
class Node:
    """A node in the lineage graph."""
    
    node_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    node_type: NodeType = NodeType.DATASET
    urn: str = ""
    name: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    discovered_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "node_id": self.node_id,
            "node_type": self.node_type.value,
            "urn": self.urn,
            "name": self.name,
            "metadata": self.metadata,
            "discovered_at": self.discovered_at.isoformat()
        }


@dataclass
class Edge:
    """An edge in the lineage graph."""
    
    edge_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_node_id: str = ""
    target_node_id: str = ""
    edge_type: EdgeType = EdgeType.READ
    confidence: float = 0.5
    evidence: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "edge_id": self.edge_id,
            "source_node_id": self.source_node_id,
            "target_node_id": self.target_node_id,
            "edge_type": self.edge_type.value,
            "confidence": self.confidence,
            "evidence": self.evidence,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat()
        }


@dataclass
class DatasetNode(Node):
    """A dataset node."""
    
    def __post_init__(self) -> None:
        self.node_type = NodeType.DATASET


@dataclass
class JobNode(Node):
    """A job node."""
    
    def __post_init__(self) -> None:
        self.node_type = NodeType.JOB


@dataclass
class ModuleNode(Node):
    """A module node."""
    
    def __post_init__(self) -> None:
        self.node_type = NodeType.MODULE

