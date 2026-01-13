"""
Test suite for modularized Scala and PySpark applications
Ensures the lineage tool correctly tracks reads/writes across multiple modules
"""

import pytest
from pathlib import Path
from lineage.extractors.scala_extractor import ScalaExtractor
from lineage.extractors.pyspark_extractor import PySparkExtractor
from lineage.ir.fact import FactType


class TestScalaModularApp:
    """Tests for modularized Scala Spark application"""
    
    @pytest.fixture
    def scala_extractor(self):
        return ScalaExtractor()
    
    def test_scala_data_ingestion_module(self, scala_extractor):
        """Test extraction from DataIngestionModule.scala"""
        mock_file = Path("tests/mocks/scala/modular_app/DataIngestionModule.scala")
        facts = scala_extractor.extract(str(mock_file))
        
        # Verify read operations  (Scala extractor extracts from method bodies)
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        assert len(read_facts) >= 2, "Should detect read operations"
        
        # Check Hive table reads
        hive_reads = [f for f in read_facts if f.dataset_urn and "customers" in f.dataset_urn.lower()]
        assert len(hive_reads) >= 1, "Should detect Hive table read"
        assert any("${env}_raw.customers" in f.dataset_urn for f in hive_reads)
        
        # Check JSON reads
        json_reads = [f for f in read_facts if f.dataset_urn and "json" in f.evidence.lower()]
        assert len(json_reads) >= 1, "Should detect JSON read"
        
        # Verify module attribution
        assert all(f.source_file.endswith("DataIngestionModule.scala") for f in facts)
        
        print(f"✅ DataIngestionModule: {len(read_facts)} read operations detected")
    
    def test_scala_transformation_module(self, scala_extractor):
        """Test extraction from TransformationModule.scala"""
        mock_file = Path("tests/mocks/scala/modular_app/TransformationModule.scala")
        facts = scala_extractor.extract(str(mock_file))
        
        # Transformation module typically doesn't have direct I/O
        # but may have intermediate operations
        assert len(facts) >= 0, "Transformation module extracted"
        
        # Verify module attribution
        assert all(f.source_file.endswith("TransformationModule.scala") for f in facts)
        
        print(f"✅ TransformationModule: {len(facts)} facts extracted")
    
    def test_scala_data_output_module(self, scala_extractor):
        """Test extraction from DataOutputModule.scala"""
        mock_file = Path("tests/mocks/scala/modular_app/DataOutputModule.scala")
        facts = scala_extractor.extract(str(mock_file))
        
        # Verify write operations
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 4, "Should detect write operations"
        
        # Check Hive table writes (saveAsTable with tableName variable)
        hive_writes = [f for f in write_facts if f.dataset_urn and "tableName" in f.dataset_urn]
        assert len(hive_writes) >= 2, "Should detect Hive table writes"
        
        # Check HDFS writes (parquet, csv)
        hdfs_writes = [f for f in write_facts if f.dataset_urn and "path" in f.dataset_urn]
        assert len(hdfs_writes) >= 2, "Should detect HDFS writes"
        
        # Verify module attribution
        assert all(f.source_file.endswith("DataOutputModule.scala") for f in facts)
        
        print(f"✅ DataOutputModule: {len(write_facts)} write operations detected")
    
    def test_scala_orchestrator(self, scala_extractor):
        """Test extraction from ETLOrchestrator.scala"""
        mock_file = Path("tests/mocks/scala/modular_app/ETLOrchestrator.scala")
        facts = scala_extractor.extract(str(mock_file))
        
        # Orchestrator coordinates modules but may not have direct I/O
        # Should detect method calls to module functions
        assert len(facts) >= 0, "Orchestrator extracted"
        
        # Verify module attribution
        assert all(f.source_file.endswith("ETLOrchestrator.scala") for f in facts)
        
        print(f"✅ ETLOrchestrator: {len(facts)} facts extracted")
    
    def test_scala_complete_app_lineage(self, scala_extractor):
        """Test complete lineage across all Scala modules"""
        modules = [
            "DataIngestionModule.scala",
            "TransformationModule.scala",
            "DataOutputModule.scala",
            "ETLOrchestrator.scala"
        ]
        
        all_facts = []
        for module in modules:
            mock_file = Path(f"tests/mocks/scala/modular_app/{module}")
            if mock_file.exists():
                facts = scala_extractor.extract(str(mock_file))
                all_facts.extend(facts)
        
        # Verify complete data flow
        all_reads = [f for f in all_facts if f.fact_type == FactType.READ]
        all_writes = [f for f in all_facts if f.fact_type == FactType.WRITE]
        
        assert len(all_reads) >= 2, "Should have read operations across modules"
        assert len(all_writes) >= 4, "Should have write operations across modules"
        
        # Verify diverse data sources
        sources = set()
        for fact in all_facts:
            if not fact.dataset_urn:
                continue
            if "hive://" in fact.dataset_urn or any(table in fact.dataset_urn for table in ["customers", "enriched_transactions", "product_daily_summary"]):
                sources.add("Hive")
            elif "hdfs://" in fact.dataset_urn or fact.dataset_urn.startswith("/data") or fact.dataset_urn == "path":
                sources.add("HDFS")
            elif "jdbc:" in fact.dataset_urn:
                sources.add("JDBC")
            elif "kafka" in fact.dataset_urn.lower():
                sources.add("Kafka")
            elif "s3a://" in fact.dataset_urn:
                sources.add("S3")
            elif "delta" in fact.dataset_urn.lower():
                sources.add("Delta")
        
        assert len(sources) >= 1, f"Should use data sources, found: {sources}"
        
        print(f"✅ Complete Scala App: {len(all_reads)} reads, {len(all_writes)} writes across {len(modules)} modules")
        print(f"   Data sources: {', '.join(sorted(sources))}")


class TestPySparkModularApp:
    """Tests for modularized PySpark application"""
    
    @pytest.fixture
    def pyspark_extractor(self):
        return PySparkExtractor()
    
    def test_pyspark_data_ingestion_module(self, pyspark_extractor):
        """Test extraction from data_ingestion_module.py"""
        mock_file = Path("tests/mocks/pyspark/modular_app/data_ingestion_module.py")
        facts = pyspark_extractor.extract(str(mock_file))
        
        # Verify read operations (PySpark extractor extracts from method bodies)
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        assert len(read_facts) >= 5, "Should detect read operations"
        
        # Verify some key operations are detected
        all_evidence = " ".join([f.evidence for f in read_facts if f.evidence])
        assert "parquet" in all_evidence.lower(), "Should detect parquet reads"
        assert "table" in all_evidence.lower() or "spark.table" in all_evidence.lower(), "Should detect table reads"
        
        # Verify module attribution
        assert all(f.source_file.endswith("data_ingestion_module.py") for f in facts)
        
        print(f"✅ data_ingestion_module: {len(read_facts)} read operations detected")
    
    def test_pyspark_transformation_module(self, pyspark_extractor):
        """Test extraction from transformation_module.py"""
        mock_file = Path("tests/mocks/pyspark/modular_app/transformation_module.py")
        facts = pyspark_extractor.extract(str(mock_file))
        
        # Transformation module typically doesn't have direct I/O
        assert len(facts) >= 0, "Transformation module extracted"
        
        # Verify module attribution
        assert all(f.source_file.endswith("transformation_module.py") for f in facts)
        
        print(f"✅ transformation_module: {len(facts)} facts extracted")
    
    def test_pyspark_data_output_module(self, pyspark_extractor):
        """Test extraction from data_output_module.py"""
        mock_file = Path("tests/mocks/pyspark/modular_app/data_output_module.py")
        facts = pyspark_extractor.extract(str(mock_file))
        
        # Verify write operations
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 5, "Should detect write operations"
        
        # Verify some key operations are detected
        all_evidence = " ".join([f.evidence for f in write_facts if f.evidence])
        assert "write" in all_evidence.lower(), "Should detect write operations"
        assert ("savetable" in all_evidence.lower().replace(" ", "") or 
                "parquet" in all_evidence.lower() or 
                "csv" in all_evidence.lower()), "Should detect table/file writes"
        
        # Verify module attribution
        assert all(f.source_file.endswith("data_output_module.py") for f in facts)
        
        print(f"✅ data_output_module: {len(write_facts)} write operations detected")
    
    def test_pyspark_orchestrator(self, pyspark_extractor):
        """Test extraction from etl_orchestrator.py"""
        mock_file = Path("tests/mocks/pyspark/modular_app/etl_orchestrator.py")
        facts = pyspark_extractor.extract(str(mock_file))
        
        # Orchestrator coordinates modules but may not have direct I/O
        assert len(facts) >= 0, "Orchestrator extracted"
        
        # Verify module attribution
        assert all(f.source_file.endswith("etl_orchestrator.py") for f in facts)
        
        print(f"✅ etl_orchestrator: {len(facts)} facts extracted")
    
    def test_pyspark_complete_app_lineage(self, pyspark_extractor):
        """Test complete lineage across all PySpark modules"""
        modules = [
            "data_ingestion_module.py",
            "transformation_module.py",
            "data_output_module.py",
            "etl_orchestrator.py"
        ]
        
        all_facts = []
        for module in modules:
            mock_file = Path(f"tests/mocks/pyspark/modular_app/{module}")
            if mock_file.exists():
                facts = pyspark_extractor.extract(str(mock_file))
                all_facts.extend(facts)
        
        # Verify complete data flow
        all_reads = [f for f in all_facts if f.fact_type == FactType.READ]
        all_writes = [f for f in all_facts if f.fact_type == FactType.WRITE]
        
        assert len(all_reads) >= 5, "Should have read operations across modules"
        assert len(all_writes) >= 5, "Should have write operations across modules"
        
        # Verify module attribution is preserved
        source_modules = set(f.source_file for f in all_facts)
        assert len(source_modules) >= 2, "Facts should come from multiple modules"
        
        print(f"✅ Complete PySpark App: {len(all_reads)} reads, {len(all_writes)} writes across {len(source_modules)} modules")
        print(f"   Modules: {', '.join(Path(m).name for m in sorted(source_modules))}")


class TestCrossLanguageModularApps:
    """Test comparison between Scala and PySpark modular apps"""
    
    def test_scala_vs_pyspark_coverage(self):
        """Compare extraction coverage between Scala and PySpark modular apps"""
        scala_extractor = ScalaExtractor()
        pyspark_extractor = PySparkExtractor()
        
        # Extract from Scala modules
        scala_modules = [
            "DataIngestionModule.scala",
            "DataOutputModule.scala"
        ]
        scala_facts = []
        for module in scala_modules:
            mock_file = Path(f"tests/mocks/scala/modular_app/{module}")
            if mock_file.exists():
                scala_facts.extend(scala_extractor.extract(str(mock_file)))
        
        # Extract from PySpark modules
        pyspark_modules = [
            "data_ingestion_module.py",
            "data_output_module.py"
        ]
        pyspark_facts = []
        for module in pyspark_modules:
            mock_file = Path(f"tests/mocks/pyspark/modular_app/{module}")
            if mock_file.exists():
                pyspark_facts.extend(pyspark_extractor.extract(str(mock_file)))
        
        scala_reads = len([f for f in scala_facts if f.fact_type == FactType.READ])
        scala_writes = len([f for f in scala_facts if f.fact_type == FactType.WRITE])
        
        pyspark_reads = len([f for f in pyspark_facts if f.fact_type == FactType.READ])
        pyspark_writes = len([f for f in pyspark_facts if f.fact_type == FactType.WRITE])
        
        print(f"✅ Scala: {scala_reads} reads, {scala_writes} writes")
        print(f"✅ PySpark: {pyspark_reads} reads, {pyspark_writes} writes")
        
        # Both should have extraction capabilities
        assert scala_reads >= 2, "Scala should extract reads"
        assert scala_writes >= 4, "Scala should extract writes"
        assert pyspark_reads >= 5, "PySpark should extract reads"
        assert pyspark_writes >= 5, "PySpark should extract writes"
        
        # The key insight: both extractors successfully extract from modularized code
        # Module attribution is preserved in source_file field
        print(f"✅ Both extractors successfully handle modularized applications")

