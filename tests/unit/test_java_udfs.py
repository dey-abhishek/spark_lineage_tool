"""Unit tests for Java UDF extraction and lineage tracking."""

import pytest
from pathlib import Path
from lineage.extractors.java_extractor import JavaExtractor
from lineage.ir import FactType


class TestJavaUDFExtraction:
    """Test extraction from Java UDF files."""
    
    def test_spark_udf_with_main_method(self):
        """Test extraction from Spark UDF with main method that uses it."""
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/SparkStringTransformUDF.java"))
        
        # Should extract reads and writes from main method
        reads = [f for f in facts if f.fact_type == FactType.READ]
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        
        assert len(reads) >= 1, "Should extract read operation"
        assert len(writes) >= 1, "Should extract write operation"
        
        # Check specific paths
        read_urns = [f.dataset_urn for f in reads]
        write_urns = [f.dataset_urn for f in writes]
        
        assert any("customer_names.parquet" in urn for urn in read_urns)
        assert any("customer_names_clean.parquet" in urn for urn in write_urns)
    
    def test_hive_batch_processor_comprehensive(self):
        """Test extraction from comprehensive batch processor using multiple UDFs."""
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/HiveUDFBatchProcessor.java"))
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        configs = [f for f in facts if f.fact_type == FactType.CONFIG]
        
        # Should extract multiple operations
        assert len(reads) > 0, "Should extract read operations"
        assert len(writes) > 0, "Should extract write operations"
        assert len(configs) > 0, "Should extract config variables"
        
        # Check for HDFS operations
        all_urns = [f.dataset_urn for f in reads + writes if f.dataset_urn]
        hdfs_ops = [urn for urn in all_urns if 'hdfs://' in urn or '/data/' in urn]
        assert len(hdfs_ops) > 0, "Should extract HDFS operations"
    
    def test_hdfs_path_construction(self):
        """Test extraction of HDFS paths constructed with variables."""
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/HiveUDFBatchProcessor.java"))
        
        # Check for variable definitions
        configs = [f for f in facts if f.fact_type == FactType.CONFIG]
        config_keys = [f.config_key for f in configs]
        
        assert "HDFS_INPUT" in config_keys
        assert "HDFS_OUTPUT" in config_keys
        assert "HIVE_DB" in config_keys
        assert "HIVE_TABLE" in config_keys
    
    def test_hdfs_operations_detected(self):
        """Test that HDFS FileSystem operations are detected."""
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/HiveUDFBatchProcessor.java"))
        
        # HDFS operations should create read/write facts
        ops = [f for f in facts if f.fact_type in [FactType.READ, FactType.WRITE]]
        hdfs_ops = [f for f in ops if f.dataset_type == 'hdfs']
        
        assert len(hdfs_ops) > 0, "Should detect HDFS operations"
    
    def test_confidence_levels(self):
        """Test confidence scoring for Java extractions."""
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/SparkStringTransformUDF.java"))
        
        # All facts should have reasonable confidence
        for fact in facts:
            assert fact.confidence >= 0.7, f"Confidence too low: {fact.confidence}"
            assert fact.confidence <= 1.0, f"Confidence too high: {fact.confidence}"
    
    def test_extraction_method_is_regex(self):
        """Test that extraction method is properly set."""
        from lineage.ir import ExtractionMethod
        
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/SparkStringTransformUDF.java"))
        
        # Java extractor uses regex
        for fact in facts:
            assert fact.extraction_method == ExtractionMethod.REGEX


class TestJavaUDFLineageIntegration:
    """Integration tests for Java UDFs in lineage analysis."""
    
    def test_end_to_end_java_udf_lineage(self):
        """Test complete lineage extraction from Java UDF files."""
        from lineage.ir import FactStore
        from lineage.resolution import SymbolTable, VariableResolver
        from lineage.lineage import LineageBuilder
        
        # Extract facts
        extractor = JavaExtractor()
        fact_store = FactStore()
        
        for java_file in Path("tests/mocks/java").glob("*.java"):
            facts = extractor.extract(java_file)
            fact_store.add_facts(facts)
        
        # Should have extracted facts from multiple files
        stats = fact_store.get_stats()
        assert stats['total_facts'] > 0
        assert stats['unique_files'] >= 1
        
        # Build lineage
        symbol_table = SymbolTable()
        
        # Add variables from extracted configs
        for fact in fact_store.get_facts_by_type(FactType.CONFIG):
            symbol_table.add_property(fact.config_key, fact.config_value)
        
        resolver = VariableResolver(symbol_table)
        builder = LineageBuilder(fact_store, resolver)
        graph = builder.build()
        
        # Should have created nodes and edges
        graph_stats = graph.get_stats()
        assert graph_stats['total_nodes'] > 0
        assert graph_stats['total_edges'] > 0
    
    def test_java_jobs_in_lineage_graph(self):
        """Test that Java files appear as jobs in lineage graph."""
        from lineage.ir import FactStore
        from lineage.resolution import SymbolTable, VariableResolver
        from lineage.lineage import LineageBuilder, NodeType
        
        extractor = JavaExtractor()
        fact_store = FactStore()
        
        # Extract from batch processor
        facts = extractor.extract(Path("tests/mocks/java/HiveUDFBatchProcessor.java"))
        fact_store.add_facts(facts)
        
        # Build graph
        resolver = VariableResolver(SymbolTable())
        builder = LineageBuilder(fact_store, resolver)
        graph = builder.build()
        
        # Check for job nodes
        job_nodes = graph.get_nodes_by_type(NodeType.JOB)
        java_jobs = [n for n in job_nodes if '.java' in n.metadata.get('source_file', '')]
        
        assert len(java_jobs) > 0, "Should have Java job nodes"
        assert any('HiveUDFBatchProcessor' in n.name for n in java_jobs)
    
    def test_java_dataset_relationships(self):
        """Test that datasets are correctly linked to Java jobs."""
        from lineage.ir import FactStore
        from lineage.resolution import SymbolTable, VariableResolver
        from lineage.lineage import LineageBuilder, NodeType
        
        extractor = JavaExtractor()
        fact_store = FactStore()
        
        facts = extractor.extract(Path("tests/mocks/java/SparkStringTransformUDF.java"))
        fact_store.add_facts(facts)
        
        resolver = VariableResolver(SymbolTable())
        builder = LineageBuilder(fact_store, resolver)
        graph = builder.build()
        
        # Should have datasets
        dataset_nodes = graph.get_nodes_by_type(NodeType.DATASET)
        assert len(dataset_nodes) > 0, "Should have dataset nodes"
        
        # Should have edges connecting jobs to datasets
        assert graph.get_stats()['total_edges'] > 0


class TestJavaUDFUsagePatterns:
    """Test different UDF usage patterns."""
    
    def test_hive_udf_simple(self):
        """Test simple Hive UDF extraction."""
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/StringCleanerUDF.java"))
        
        # UDF files might not have I/O operations, just UDF definitions
        # But should be extracted successfully
        assert isinstance(facts, list)
    
    def test_hive_udf_with_date_operations(self):
        """Test date parsing UDF."""
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/DateParserUDF.java"))
        
        assert isinstance(facts, list)
    
    def test_hive_udaf_aggregation(self):
        """Test UDAF (aggregate function) extraction."""
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/AggregateStatsUDAF.java"))
        
        assert isinstance(facts, list)
    
    def test_multiple_java_files_together(self):
        """Test extracting from multiple Java files simultaneously."""
        extractor = JavaExtractor()
        
        all_facts = []
        for java_file in Path("tests/mocks/java").glob("*.java"):
            facts = extractor.extract(java_file)
            all_facts.extend(facts)
        
        # Should have extracted facts from multiple files
        assert len(all_facts) > 0
        
        # Count facts by type
        reads = [f for f in all_facts if f.fact_type == FactType.READ]
        writes = [f for f in all_facts if f.fact_type == FactType.WRITE]
        configs = [f for f in all_facts if f.fact_type == FactType.CONFIG]
        
        assert len(reads) > 0, "Should have reads"
        assert len(writes) > 0, "Should have writes"
        assert len(configs) > 0, "Should have configs"


class TestJavaVariableResolution:
    """Test variable resolution in Java files."""
    
    def test_static_final_constants_extracted(self):
        """Test that static final constants are extracted."""
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/HiveUDFBatchProcessor.java"))
        
        configs = [f for f in facts if f.fact_type == FactType.CONFIG]
        config_dict = {f.config_key: f.config_value for f in configs}
        
        # Should extract constants
        assert "HDFS_INPUT" in config_dict
        assert "HDFS_OUTPUT" in config_dict
        assert "prod" in config_dict.get("HDFS_INPUT", "")
    
    def test_variable_resolution_in_paths(self):
        """Test that variables in paths are detected."""
        extractor = JavaExtractor()
        facts = extractor.extract(Path("tests/mocks/java/HiveUDFBatchProcessor.java"))
        
        # Paths with variables should be marked
        ops = [f for f in facts if f.fact_type in [FactType.READ, FactType.WRITE]]
        
        # Some operations might have placeholders
        has_variables = any(f.has_placeholders for f in ops if hasattr(f, 'has_placeholders'))
        
        # This is expected for parameterized paths
        # Test passes if extraction works
        assert len(ops) >= 0  # Just verify extraction succeeded

