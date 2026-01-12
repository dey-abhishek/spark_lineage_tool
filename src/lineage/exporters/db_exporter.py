"""Database exporter for lineage data."""

from pathlib import Path
from typing import Dict, Any, Optional
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, DateTime, JSON, Text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from lineage.lineage import LineageGraph
from lineage.config import DatabaseConfig


class DatabaseExporter:
    """Export lineage graph to database."""
    
    def __init__(self, db_config: DatabaseConfig) -> None:
        self.config = db_config
        self.engine = None
        self.metadata = MetaData()
        
        # Define tables
        self.nodes_table = Table(
            'nodes', self.metadata,
            Column('node_id', String(255), primary_key=True),
            Column('node_type', String(50)),
            Column('urn', String(1000)),
            Column('name', String(500)),
            Column('metadata', JSON),
            Column('discovered_at', DateTime)
        )
        
        self.edges_table = Table(
            'edges', self.metadata,
            Column('edge_id', String(255), primary_key=True),
            Column('source_node_id', String(255)),
            Column('target_node_id', String(255)),
            Column('edge_type', String(50)),
            Column('confidence', Float),
            Column('evidence', Text),
            Column('created_at', DateTime)
        )
        
        self.metrics_table = Table(
            'metrics', self.metadata,
            Column('node_id', String(255), primary_key=True),
            Column('fan_in', Integer),
            Column('fan_out', Integer),
            Column('downstream_reach', Integer),
            Column('priority_score', Float),
            Column('migration_wave', Integer)
        )
    
    def connect(self) -> bool:
        """Connect to database."""
        if not self.config.enabled:
            return False
        
        try:
            # Build connection string
            if self.config.driver == "sqlite":
                conn_str = f"sqlite:///{self.config.database}"
            else:
                conn_str = (
                    f"{self.config.driver}://{self.config.username}:{self.config.password}"
                    f"@{self.config.host}:{self.config.port}/{self.config.database}"
                )
            
            self.engine = create_engine(conn_str)
            self.metadata.create_all(self.engine)
            return True
        
        except Exception as e:
            print(f"Failed to connect to database: {e}")
            return False
    
    def export(
        self,
        graph: LineageGraph,
        metrics: Dict[str, Any],
        output_path: Optional[Path] = None
    ) -> None:
        """Export graph to database."""
        if not self.connect():
            print("Skipping database export (not configured)")
            return
        
        with self.engine.begin() as conn:
            # Export nodes
            for node in graph.nodes.values():
                conn.execute(
                    self.nodes_table.insert().values(
                        node_id=node.node_id,
                        node_type=node.node_type.value,
                        urn=node.urn,
                        name=node.name,
                        metadata=node.metadata,
                        discovered_at=node.discovered_at
                    )
                )
            
            # Export edges
            for edge in graph.edges:
                conn.execute(
                    self.edges_table.insert().values(
                        edge_id=edge.edge_id,
                        source_node_id=edge.source_node_id,
                        target_node_id=edge.target_node_id,
                        edge_type=edge.edge_type.value,
                        confidence=edge.confidence,
                        evidence=edge.evidence,
                        created_at=edge.created_at
                    )
                )
            
            # Export metrics
            for node_id, m in metrics.items():
                conn.execute(
                    self.metrics_table.insert().values(
                        node_id=node_id,
                        fan_in=m.fan_in,
                        fan_out=m.fan_out,
                        downstream_reach=m.downstream_reach,
                        priority_score=m.priority_score,
                        migration_wave=m.migration_wave
                    )
                )
        
        print("Exported lineage to database")

