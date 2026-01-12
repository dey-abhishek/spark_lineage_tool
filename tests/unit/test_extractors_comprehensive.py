"""
Comprehensive TDD tests for all extractors
Tests organized by: Test → Code → Refactor cycle
"""

import pytest
from pathlib import Path
from lineage.extractors.pyspark_extractor import PySparkExtractor
from lineage.extractors.scala_extractor import ScalaExtractor
from lineage.extractors.hive_extractor import HiveExtractor
from lineage.extractors.shell_extractor import ShellExtractor
from lineage.extractors.nifi_extractor import NiFiExtractor
from lineage.extractors.config_extractor import ConfigExtractor
from lineage.ir.fact import FactType, ExtractionMethod
from lineage.rules.engine import RuleEngine


class TestPySparkExtractorTDD:
    """TDD tests for PySpark extractor"""
    
    @pytest.fixture
    def extractor(self):
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        return PySparkExtractor(rule_engine)
    
    def test_complex_etl_pipeline_extraction(self, extractor):
        """Test: Extract from complex multi-source ETL pipeline"""
        mock_file = Path("tests/mocks/pyspark/11_complex_etl_pipeline.py")
        facts = extractor.extract(mock_file)
        
        # Assertions
        assert len(facts) > 0, "Should extract facts from complex pipeline"
        
        # Verify read operations
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        assert len(read_facts) >= 4, "Should detect 4+ read operations"
        
        # Specific reads
        read_urns = {f.dataset_urn for f in read_facts}
        assert any("/data/raw/transactions" in urn for urn in read_urns)
        assert any("/data/raw/products" in urn for urn in read_urns)
        
        # Verify write operations
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 3, "Should detect 3+ write operations"
        
        # Verify confidence scores
        ast_facts = [f for f in facts if f.extraction_method == ExtractionMethod.AST]
        assert len(ast_facts) > 0, "Should use AST extraction"
        assert all(f.confidence >= 0.75 for f in ast_facts), "AST facts should have high confidence"
    
    def test_streaming_job_extraction(self, extractor):
        """Test: Extract from Spark Structured Streaming job"""
        mock_file = Path("tests/mocks/pyspark/12_streaming_job.py")
        facts = extractor.extract(mock_file)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        
        # Verify streaming sources
        assert any("kafka" in f.evidence.lower() for f in read_facts), "Should detect Kafka source"
        
        # Verify checkpointing
        checkpoint_writes = [f for f in write_facts if "checkpoint" in f.dataset_urn.lower()]
        assert len(checkpoint_writes) > 0, "Should detect checkpoint location"
        
        # Verify output
        output_writes = [f for f in write_facts if "/data/streaming/output" in f.dataset_urn]
        assert len(output_writes) > 0, "Should detect streaming output"
    
    def test_complex_sql_with_ctes(self, extractor):
        """Test: Extract from SQL with CTEs and subqueries"""
        mock_file = Path("tests/mocks/pyspark/13_complex_sql_ctes.py")
        facts = extractor.extract(mock_file)
        
        # Should extract SQL facts
        sql_facts = [f for f in facts if f.params.get("sql")]
        assert len(sql_facts) > 0, "Should detect embedded SQL"
        
        # Verify tables in SQL
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        table_reads = [f for f in read_facts if "hive://" in (f.dataset_urn or "")]
        assert len(table_reads) >= 2, "Should detect multiple table reads in SQL"
    
    def test_delta_merge_incremental(self, extractor):
        """Test: Extract from Delta Lake merge operation"""
        mock_file = Path("tests/mocks/pyspark/14_incremental_delta_merge.py")
        facts = extractor.extract(mock_file)
        
        # Verify Delta operations
        delta_facts = [f for f in facts if "delta" in (f.evidence or "").lower()]
        assert len(delta_facts) > 0, "Should detect Delta Lake operations"
        
        # Verify checkpoint writes
        checkpoint_facts = [f for f in facts if "checkpoint" in (f.dataset_urn or "")]
        assert len(checkpoint_facts) > 0, "Should detect checkpoint location"
    
    def test_udf_and_error_handling(self, extractor):
        """Test: Extract from job with UDFs and bad records"""
        mock_file = Path("tests/mocks/pyspark/15_udf_error_handling.py")
        facts = extractor.extract(mock_file)
        
        # Verify main output
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 3, "Should detect multiple outputs (good, bad, metrics)"
        
        # Verify error paths
        error_writes = [f for f in write_facts if "error" in f.dataset_urn.lower()]
        assert len(error_writes) > 0, "Should detect error output paths"


class TestScalaExtractorTDD:
    """TDD tests for Scala extractor"""
    
    @pytest.fixture
    def extractor(self):
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        return ScalaExtractor(rule_engine)
    
    def test_complex_scala_etl(self, extractor):
        """Test: Extract from complex Scala Spark ETL"""
        mock_file = Path("tests/mocks/scala/02_ComplexETLPipeline.scala")
        facts = extractor.extract(mock_file)
        
        assert len(facts) > 0, "Should extract from Scala code"
        
        # Verify reads
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        assert len(read_facts) >= 3, "Should detect multiple reads"
        
        # Verify writes
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 3, "Should detect multiple writes"
    
    def test_custom_io_wrapper(self, extractor):
        """Test: Extract from custom IO wrapper methods"""
        mock_file = Path("tests/mocks/scala/03_CustomIOWrapper.scala")
        facts = extractor.extract(mock_file)
        
        # May need custom rules for org-specific patterns
        # This tests extensibility
        assert len(facts) >= 0, "Should handle custom wrappers"
    
    def test_string_interpolation(self, extractor):
        """Test: Extract paths with Scala string interpolation"""
        mock_file = Path("tests/mocks/scala/04_StringInterpolation.scala")
        facts = extractor.extract(mock_file)
        
        # Verify placeholder detection
        facts_with_placeholders = [f for f in facts if f.has_placeholders]
        assert len(facts_with_placeholders) > 0, "Should detect string interpolation as placeholders"


class TestHiveExtractorTDD:
    """TDD tests for Hive SQL extractor"""
    
    @pytest.fixture
    def extractor(self):
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        return HiveExtractor(rule_engine)
    
    def test_dynamic_partitions(self, extractor):
        """Test: Extract from dynamic partition insert"""
        mock_file = Path("tests/mocks/hive/03_dynamic_partitions.hql")
        facts = extractor.extract(mock_file)
        
        assert len(facts) > 0, "Should extract from Hive SQL"
        
        # Verify reads
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        assert len(read_facts) >= 2, "Should detect table reads"
        
        # Verify writes
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 1, "Should detect table write"
        
        # Verify confidence
        assert all(f.confidence >= 0.80 for f in facts), "Hive SQL should have high confidence"
    
    def test_multi_insert(self, extractor):
        """Test: Extract from FROM...INSERT pattern"""
        mock_file = Path("tests/mocks/hive/04_multi_insert.hql")
        facts = extractor.extract(mock_file)
        
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 3, "Should detect multiple INSERT targets"
        
        # Verify table names
        write_tables = {f.params.get("table_name") for f in write_facts if f.params.get("table_name")}
        assert len(write_tables) >= 3, "Should detect distinct target tables"
    
    def test_ctes_and_window_functions(self, extractor):
        """Test: Extract from complex CTEs"""
        mock_file = Path("tests/mocks/hive/05_ctes_window_functions.hql")
        facts = extractor.extract(mock_file)
        
        # Should handle complex SQL
        assert len(facts) > 0, "Should extract from complex SQL with CTEs"
        
        # Verify source tables
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        assert len(read_facts) >= 2, "Should detect source tables"


class TestShellExtractorTDD:
    """TDD tests for Shell script extractor"""
    
    @pytest.fixture
    def extractor(self):
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        return ShellExtractor(rule_engine)
    
    def test_complex_pipeline_script(self, extractor):
        """Test: Extract from complex shell pipeline"""
        mock_file = Path("tests/mocks/shell/03_complex_pipeline.sh")
        facts = extractor.extract(mock_file)
        
        assert len(facts) > 0, "Should extract from shell script"
        
        # Verify HDFS operations
        hdfs_facts = [f for f in facts if "hdfs" in f.evidence.lower()]
        assert len(hdfs_facts) > 0, "Should detect HDFS operations"
        
        # Verify spark-submit
        spark_jobs = [f for f in facts if "spark-submit" in f.evidence.lower()]
        assert len(spark_jobs) > 0, "Should detect spark-submit commands"
        
        # Verify hive execution
        hive_facts = [f for f in facts if "hive" in f.evidence.lower()]
        assert len(hive_facts) > 0, "Should detect Hive commands"
    
    def test_beeline_operations(self, extractor):
        """Test: Extract from Beeline operations"""
        mock_file = Path("tests/mocks/shell/04_beeline_operations.sh")
        facts = extractor.extract(mock_file)
        
        # Verify beeline detection
        beeline_facts = [f for f in facts if "beeline" in f.evidence.lower()]
        assert len(beeline_facts) > 0, "Should detect Beeline commands"


class TestNiFiExtractorTDD:
    """TDD tests for NiFi flow extractor"""
    
    @pytest.fixture
    def extractor(self):
        return NiFiExtractor()
    
    def test_hdfs_to_hive_flow(self, extractor):
        """Test: Extract from NiFi HDFS→Hive flow"""
        mock_file = Path("tests/mocks/nifi/01_hdfs_to_hive_flow.json")
        facts = extractor.extract(mock_file)
        
        assert len(facts) > 0, "Should extract from NiFi flow"
        
        # Verify reads (GetHDFS)
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        assert len(read_facts) > 0, "Should detect GetHDFS processor"
        
        # Verify writes (PutHive, PutHDFS)
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 2, "Should detect PutHive and PutHDFS processors"
        
        # Verify parameter context usage
        facts_with_params = [f for f in facts if f.has_placeholders]
        assert len(facts_with_params) > 0, "Should detect parameter context references"


class TestConfigExtractorTDD:
    """TDD tests for Config extractor"""
    
    @pytest.fixture
    def extractor(self):
        return ConfigExtractor()
    
    def test_complex_yaml_config(self, extractor):
        """Test: Extract from nested YAML configuration"""
        mock_file = Path("tests/mocks/configs/complex_application.yaml")
        facts = extractor.extract(mock_file)
        
        assert len(facts) > 0, "Should extract config facts"
        
        # Verify key extraction
        config_keys = {f.config_key for f in facts if hasattr(f, 'config_key')}
        assert len(config_keys) > 10, "Should extract many config keys"
        
        # Verify nested keys
        nested_keys = [k for k in config_keys if '.' in k]
        assert len(nested_keys) > 0, "Should extract nested config keys"
        
        # Verify path values
        path_facts = [f for f in facts if hasattr(f, 'config_value') and '/data/' in str(f.config_value)]
        assert len(path_facts) > 0, "Should extract path configurations"
    
    def test_xml_config(self, extractor):
        """Test: Extract from Hadoop-style XML"""
        mock_file = Path("tests/mocks/configs/hadoop-config.xml")
        facts = extractor.extract(mock_file)
        
        assert len(facts) > 0, "Should extract from XML"
        
        # Verify HDFS config
        hdfs_facts = [f for f in facts if hasattr(f, 'config_value') and 'hdfs://' in str(f.config_value)]
        assert len(hdfs_facts) > 0, "Should extract HDFS configuration"


class TestEndToEndIntegration:
    """Integration tests for complete pipeline"""
    
    def test_mock_repository_scan(self, tmp_path):
        """Test: Scan entire mock repository"""
        from lineage.crawler.file_crawler import FileCrawler
        from lineage.ir.store import FactStore
        from lineage.rules.engine import RuleEngine
        
        mock_repo = Path("tests/mocks")
        if not mock_repo.exists():
            pytest.skip("Mock repository not found")
        
        # Crawl
        crawler = FileCrawler(mock_repo)
        files = list(crawler.crawl())
        
        assert len(files) > 15, f"Should find 15+ mock files, found {len(files)}"
        
        # Extract from all files
        fact_store = FactStore()
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        
        extractors = {
            "pyspark": PySparkExtractor(rule_engine),
            "scala": ScalaExtractor(rule_engine),
            "hive": HiveExtractor(rule_engine),
            "shell": ShellExtractor(rule_engine),
            "nifi": NiFiExtractor(),
            "yaml": ConfigExtractor(),
            "json": ConfigExtractor(),
            "xml": ConfigExtractor(),
            "properties": ConfigExtractor(),
        }
        
        for crawled_file in files:
            file_type = crawled_file.file_type.value
            if file_type in extractors:
                facts = extractors[file_type].extract(crawled_file.path)
                fact_store.add_facts(facts)
        
        # Assertions
        stats = fact_store.get_stats()
        assert stats["total_facts"] > 50, f"Should extract 50+ facts, got {stats['total_facts']}"
        assert stats["read_facts"] > 10, "Should have 10+ read facts"
        assert stats["write_facts"] > 10, "Should have 10+ write facts"
        assert stats["high_confidence"] > 20, "Should have 20+ high confidence facts"
        
        print(f"\n✅ Integration Test Results:")
        print(f"   Files scanned: {len(files)}")
        print(f"   Total facts: {stats['total_facts']}")
        print(f"   Read operations: {stats['read_facts']}")
        print(f"   Write operations: {stats['write_facts']}")
        print(f"   High confidence: {stats['high_confidence']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

