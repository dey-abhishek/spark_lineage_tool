"""Priority and wave calculation for migration planning."""

from typing import Dict, List, Tuple
from dataclasses import dataclass
from lineage.lineage import LineageGraph, Node, NodeType


@dataclass
class PriorityMetrics:
    """Priority metrics for a node."""
    
    node_id: str
    fan_in: int
    fan_out: int
    downstream_reach: int
    write_job_count: int
    avg_confidence: float
    priority_score: float
    migration_wave: int
    is_shell_only: bool


class PriorityCalculator:
    """Calculates priority scores and migration waves."""
    
    def __init__(self, graph: LineageGraph) -> None:
        self.graph = graph
        self.metrics: Dict[str, PriorityMetrics] = {}
    
    def calculate_all(self) -> Dict[str, PriorityMetrics]:
        """Calculate metrics for all dataset nodes."""
        for node in self.graph.get_nodes_by_type(NodeType.DATASET):
            # Skip wildcard patterns and temporary paths
            if self._should_skip_node(node):
                continue
                
            metrics = self.calculate_node_metrics(node)
            self.metrics[node.node_id] = metrics
        
        # Assign migration waves
        self._assign_migration_waves()
        
        return self.metrics
    
    def _should_skip_node(self, node: Node) -> bool:
        """Check if node should be skipped from priority calculation."""
        name = node.name.lower()
        urn = node.urn.lower()
        
        # Skip wildcard patterns
        if '*' in name or '*' in urn:
            return True
        
        # Skip unresolved variables (placeholders)
        if '${' in name or '${' in urn or '$(' in name or '$(' in urn:
            # But keep if it's marked as fully resolved in metadata
            if not node.metadata.get('fully_resolved', False):
                return True
        
        # Skip temporary/staging paths
        temp_patterns = ['_tmp', '_temp', '_staging', '_checkpoint', '_success', '_logs', '_metadata']
        if any(pattern in name or pattern in urn for pattern in temp_patterns):
            return True
        
        # Skip very generic names
        generic_names = ['data', 'tmp', 'temp', 'staging', 'output', 'input', '-update', 'events']
        if name in generic_names:
            return True
        
        # Skip very short names (likely partial extractions)
        if len(name) < 3:
            return True
        
        # Skip variable names that don't look like paths or tables
        # These are likely unresolved variable identifiers
        if not self._looks_like_dataset(name, urn, node):
            return True
        
        return False
    
    def _looks_like_dataset(self, name: str, urn: str, node: Node) -> bool:
        """Check if name/URN looks like an actual dataset vs a variable identifier.
        
        Returns True if it looks like a real dataset, False if it looks like a variable name.
        """
        # If it's a Hive table with schema.table format, it's valid
        if '.' in name and node.metadata.get('dataset_type') == 'hive':
            return True
        
        # If URN has a protocol/scheme, it's likely valid (hdfs://, jdbc://, kafka://, s3://)
        if '://' in urn:
            return True
        
        # If it has path separators, it's likely a path
        if '/' in name or '\\' in name:
            return True
        
        # If it has file extensions, it's likely a file
        if '.' in name and any(ext in name for ext in ['.csv', '.json', '.parquet', '.orc', '.avro', '.txt']):
            return True
        
        # Check for camelCase or variable-like patterns (likely unresolved variables)
        # Examples: hdfsDir, inputPath, outputTable, dataFrame
        if self._looks_like_variable_name(name):
            return False
        
        # If name contains underscores and is all lowercase (common in tables), it's likely valid
        # Examples: customer_summary, sales_data, user_events
        if '_' in name and name.islower():
            return True
        
        # If it's very short (< 8 chars) and has no path indicators, likely a variable
        if len(name) < 8 and '/' not in name and '.' not in name:
            return False
        
        # Default: assume it's valid
        return True
    
    def _looks_like_variable_name(self, name: str) -> bool:
        """Check if name looks like a variable identifier (camelCase, etc.).
        
        Returns True if it looks like a variable name, False otherwise.
        """
        # Check for camelCase pattern (lowercase followed by uppercase)
        # Examples: hdfsDir, inputPath, outputTable, dataFrame
        has_camel_case = any(
            name[i].islower() and name[i+1].isupper() 
            for i in range(len(name) - 1)
        )
        if has_camel_case:
            return True
        
        # Check for common variable name patterns
        variable_patterns = [
            'dir', 'path', 'file', 'table', 'input', 'output', 
            'source', 'target', 'dest', 'src', 'config', 'var',
            'df', 'rdd', 'dataset', 'data', 'stream'
        ]
        
        # If it ends with these common variable suffixes, it's likely a variable
        for pattern in variable_patterns:
            if name.endswith(pattern) and len(name) < 15:
                return True
        
        return False
    
    def calculate_node_metrics(self, node: Node) -> PriorityMetrics:
        """Calculate priority metrics for a single node."""
        fan_in = self.graph.get_fan_in(node.node_id)
        fan_out = self.graph.get_fan_out(node.node_id)
        downstream_reach = self.graph.get_downstream_reach(node.node_id)
        
        # Count write jobs
        upstream_nodes = self.graph.get_upstream_nodes(node.node_id)
        write_job_count = sum(
            1 for n in upstream_nodes if n.node_type == NodeType.JOB
        )
        
        # Calculate average confidence
        edges = [e for e in self.graph.edges if e.target_node_id == node.node_id]
        avg_confidence = (
            sum(e.confidence for e in edges) / len(edges) if edges else 0.5
        )
        
        # Check if shell-only
        is_shell_only = all(
            "shell" in n.metadata.get("source_file", "").lower()
            for n in upstream_nodes if n.node_type == NodeType.JOB
        )
        
        # Calculate priority score
        shell_penalty = 2.0 if is_shell_only else 0.0
        priority_score = (
            3.0 * downstream_reach
            + 2.0 * fan_out
            + 1.0 * write_job_count
            - shell_penalty
        ) * avg_confidence
        
        return PriorityMetrics(
            node_id=node.node_id,
            fan_in=fan_in,
            fan_out=fan_out,
            downstream_reach=downstream_reach,
            write_job_count=write_job_count,
            avg_confidence=avg_confidence,
            priority_score=priority_score,
            migration_wave=0,  # Will be assigned later
            is_shell_only=is_shell_only
        )
    
    def _assign_migration_waves(self) -> None:
        """Assign migration waves based on priority and dependencies."""
        # Sort nodes by priority score
        sorted_metrics = sorted(
            self.metrics.values(),
            key=lambda m: m.priority_score,
            reverse=True
        )
        
        # Simple wave assignment (can be more sophisticated)
        for i, metrics in enumerate(sorted_metrics):
            if metrics.priority_score > 50 and metrics.avg_confidence > 0.7:
                metrics.migration_wave = 1  # High priority, high confidence
            elif metrics.priority_score > 20 or metrics.avg_confidence > 0.6:
                metrics.migration_wave = 2  # Medium priority
            else:
                metrics.migration_wave = 3  # Low priority
            
            # Adjust for shell-only datasets
            if metrics.is_shell_only:
                metrics.migration_wave = max(metrics.migration_wave, 3)
    
    def get_top_priority_nodes(self, n: int = 10) -> List[PriorityMetrics]:
        """Get top N priority nodes."""
        return sorted(
            self.metrics.values(),
            key=lambda m: m.priority_score,
            reverse=True
        )[:n]
    
    def get_nodes_by_wave(self, wave: int) -> List[PriorityMetrics]:
        """Get all nodes in a migration wave."""
        return [m for m in self.metrics.values() if m.migration_wave == wave]

