"""Lineage graph builder from IR facts."""

from typing import Dict, List, Set
from pathlib import Path

from .models import Node, Edge, DatasetNode, JobNode, ModuleNode, EdgeType, NodeType
from .graph import LineageGraph
from lineage.ir import FactStore, FactType
from lineage.resolution import VariableResolver


class LineageBuilder:
    """Builds lineage graph from extracted facts."""
    
    def __init__(
        self,
        fact_store: FactStore,
        resolver: VariableResolver,
        ignore_temp_paths: bool = True
    ) -> None:
        self.fact_store = fact_store
        self.resolver = resolver
        self.ignore_temp_paths = ignore_temp_paths
        self.graph = LineageGraph()
    
    def build(self) -> LineageGraph:
        """Build the lineage graph."""
        # Create nodes
        self._create_dataset_nodes()
        self._create_job_nodes()
        self._create_module_nodes()
        
        # Create edges
        self._create_job_dataset_edges()
        self._create_dataset_dataset_edges()
        self._create_job_job_edges()
        
        return self.graph
    
    def _create_dataset_nodes(self) -> None:
        """Create dataset nodes from facts."""
        unique_datasets = self.fact_store.get_unique_datasets()
        
        for dataset_urn in unique_datasets:
            # Skip temp paths if configured
            if self.ignore_temp_paths and self.resolver.canonicalizer.is_temp_path(dataset_urn):
                continue
            
            # Resolve the URN
            resolved_urn, fully_resolved = self.resolver.resolve_path(dataset_urn)
            
            # Get facts for this dataset
            facts = self.fact_store.get_facts_by_dataset(dataset_urn)
            
            # Determine dataset type
            dataset_type = facts[0].dataset_type if facts else "unknown"
            
            node = DatasetNode(
                urn=resolved_urn,
                name=self._get_dataset_name(resolved_urn),
                metadata={
                    "original_urn": dataset_urn,
                    "fully_resolved": fully_resolved,
                    "dataset_type": dataset_type,
                    "fact_count": len(facts)
                }
            )
            
            self.graph.add_node(node)
    
    def _create_job_nodes(self) -> None:
        """Create job nodes from source files."""
        unique_files = self.fact_store.get_unique_files()
        
        for source_file in unique_files:
            facts = self.fact_store.get_facts_by_file(source_file)
            
            # Extract module name from file path
            module_name = self._extract_module_name(source_file)
            
            node = JobNode(
                urn=f"job://{source_file}",
                name=Path(source_file).name,
                metadata={
                    "source_file": source_file,
                    "module": module_name,
                    "fact_count": len(facts)
                }
            )
            
            self.graph.add_node(node)
    
    def _create_module_nodes(self) -> None:
        """Create module nodes from job modules."""
        modules: Set[str] = set()
        
        for node in self.graph.get_nodes_by_type(NodeType.JOB):
            module = node.metadata.get("module")
            if module:
                modules.add(module)
        
        for module in modules:
            node = ModuleNode(
                urn=f"module://{module}",
                name=module,
                metadata={"module_path": module}
            )
            self.graph.add_node(node)
    
    def _create_job_dataset_edges(self) -> None:
        """Create edges between jobs and datasets."""
        for source_file in self.fact_store.get_unique_files():
            job_node = self.graph.get_node_by_urn(f"job://{source_file}")
            if not job_node:
                continue
            
            facts = self.fact_store.get_facts_by_file(source_file)
            
            for fact in facts:
                if not fact.dataset_urn:
                    continue
                
                # Resolve URN
                resolved_urn, _ = self.resolver.resolve_path(fact.dataset_urn)
                dataset_node = self.graph.get_node_by_urn(resolved_urn)
                
                if not dataset_node:
                    continue
                
                # Create edge based on fact type
                if fact.fact_type == FactType.READ:
                    edge = Edge(
                        source_node_id=dataset_node.node_id,
                        target_node_id=job_node.node_id,
                        edge_type=EdgeType.READ,
                        confidence=fact.confidence,
                        evidence=fact.evidence
                    )
                elif fact.fact_type == FactType.WRITE:
                    edge = Edge(
                        source_node_id=job_node.node_id,
                        target_node_id=dataset_node.node_id,
                        edge_type=EdgeType.WRITE,
                        confidence=fact.confidence,
                        evidence=fact.evidence
                    )
                else:
                    continue
                
                self.graph.add_edge(edge)
    
    def _create_dataset_dataset_edges(self) -> None:
        """Create dataset-to-dataset edges (data flow)."""
        # For each job, connect its read datasets to write datasets
        for job_node in self.graph.get_nodes_by_type(NodeType.JOB):
            source_file = job_node.metadata.get("source_file")
            if not source_file:
                continue
            
            facts = self.fact_store.get_facts_by_file(source_file)
            
            # Get read and write datasets
            read_datasets = []
            write_datasets = []
            
            for fact in facts:
                if not fact.dataset_urn:
                    continue
                
                resolved_urn, _ = self.resolver.resolve_path(fact.dataset_urn)
                dataset_node = self.graph.get_node_by_urn(resolved_urn)
                
                if not dataset_node:
                    continue
                
                if fact.fact_type == FactType.READ:
                    read_datasets.append((dataset_node, fact))
                elif fact.fact_type == FactType.WRITE:
                    write_datasets.append((dataset_node, fact))
            
            # Create edges from each read to each write
            for read_node, read_fact in read_datasets:
                for write_node, write_fact in write_datasets:
                    edge = Edge(
                        source_node_id=read_node.node_id,
                        target_node_id=write_node.node_id,
                        edge_type=EdgeType.PRODUCES,
                        confidence=min(read_fact.confidence, write_fact.confidence),
                        evidence=f"{read_fact.source_file}",
                        metadata={"job": job_node.urn}
                    )
                    self.graph.add_edge(edge)
    
    def _create_job_job_edges(self) -> None:
        """Create job-to-job dependency edges."""
        # Based on job invocation facts
        for fact in self.fact_store.get_facts_by_type(FactType.JOB_DEPENDENCY):
            source_job = self.graph.get_node_by_urn(f"job://{fact.source_file}")
            if not source_job:
                continue
            
            if fact.dependency_job:
                target_job = self.graph.get_node_by_urn(f"job://{fact.dependency_job}")
                if target_job:
                    edge = Edge(
                        source_node_id=source_job.node_id,
                        target_node_id=target_job.node_id,
                        edge_type=EdgeType.DEPENDS_ON,
                        confidence=fact.confidence,
                        evidence=fact.evidence
                    )
                    self.graph.add_edge(edge)
    
    def _get_dataset_name(self, urn: str) -> str:
        """Extract dataset name from URN."""
        # Extract last part of path or table name
        if "://" in urn:
            urn = urn.split("://", 1)[1]
        
        if "/" in urn:
            return urn.split("/")[-1] or urn.split("/")[-2]
        
        return urn.split(".")[-1] if "." in urn else urn
    
    def _extract_module_name(self, file_path: str) -> str:
        """Extract module/package name from file path."""
        path = Path(file_path)
        
        # Try to find common package roots
        parts = path.parts
        
        for i, part in enumerate(parts):
            if part in ["src", "main", "python", "scala", "java"]:
                if i + 1 < len(parts):
                    return ".".join(parts[i+1:-1])
        
        # Fallback to parent directory
        return path.parent.name if path.parent.name else "unknown"

