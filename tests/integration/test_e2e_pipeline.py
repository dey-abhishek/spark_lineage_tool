"""Integration test for end-to-end lineage extraction."""

import pytest
from pathlib import Path
from lineage.crawler import FileCrawler
from lineage.extractors import PySparkExtractor, HiveExtractor
from lineage.ir import FactStore
from lineage.rules import create_default_engine
from lineage.resolution import SymbolTable, PathCanonicalizer, VariableResolver
from lineage.lineage import LineageBuilder
from lineage.scoring import PriorityCalculator


class TestE2ELineage:
    def test_end_to_end_pipeline(self, tmp_path):
        """Test complete lineage extraction pipeline."""
        # Create mock repository
        mock_repo = tmp_path / "mock_repo"
        mock_repo.mkdir()
        
        # Create a simple PySpark file
        pyspark_file = mock_repo / "job.py"
        pyspark_file.write_text('''
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data/raw/input")
df.write.parquet("/data/processed/output")
''')
        
        # Crawl
        crawler = FileCrawler(mock_repo)
        files = list(crawler.crawl())
        assert len(files) > 0
        
        # Extract
        fact_store = FactStore()
        extractor = PySparkExtractor(create_default_engine())
        for file in files:
            facts = extractor.extract(file.path)
            fact_store.add_facts(facts)
        
        assert fact_store.get_stats()["total_facts"] > 0
        
        # Resolve
        symbol_table = SymbolTable()
        canonicalizer = PathCanonicalizer()
        resolver = VariableResolver(symbol_table, canonicalizer)
        
        # Build lineage
        builder = LineageBuilder(fact_store, resolver)
        graph = builder.build()
        
        assert graph.get_stats()["total_nodes"] > 0
        assert graph.get_stats()["total_edges"] > 0
        
        # Calculate priorities
        priority_calc = PriorityCalculator(graph)
        metrics = priority_calc.calculate_all()
        
        assert len(metrics) > 0

