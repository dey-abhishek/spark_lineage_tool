"""Unit tests for extractors."""

import pytest
from pathlib import Path
from lineage.extractors import PySparkExtractor, HiveExtractor, ScalaExtractor, ShellExtractor
from lineage.ir import FactType
from lineage.rules import create_default_engine


class TestPySparkExtractor:
    def setup_method(self):
        self.extractor = PySparkExtractor(create_default_engine())
    
    def test_simple_read_parquet(self):
        content = '''
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("/data/raw/users")
'''
        facts = self.extractor.extract_from_content(content, "test.py")
        assert len(facts) > 0
        assert any(f.fact_type == FactType.READ for f in facts)
        assert any("/data/raw/users" in f.dataset_urn for f in facts)
    
    def test_simple_write_parquet(self):
        content = '''
df.write.mode("overwrite").parquet("/data/processed/output")
'''
        facts = self.extractor.extract_from_content(content, "test.py")
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) > 0


class TestHiveExtractor:
    def setup_method(self):
        self.extractor = HiveExtractor(create_default_engine())
    
    def test_insert_overwrite(self):
        sql = "INSERT OVERWRITE TABLE analytics.summary SELECT * FROM prod.transactions"
        facts = self.extractor.extract_from_content(sql, "test.hql")
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        
        assert len(read_facts) > 0
        assert len(write_facts) > 0
    
    def test_create_table_as_select(self):
        sql = "CREATE TABLE analytics.new_table AS SELECT * FROM prod.source_table"
        facts = self.extractor.extract_from_content(sql, "test.hql")
        assert len(facts) > 0


class TestScalaExtractor:
    def setup_method(self):
        self.extractor = ScalaExtractor(create_default_engine())
    
    def test_scala_read_parquet(self):
        content = '''
val df = spark.read.parquet("/data/raw/users")
'''
        facts = self.extractor.extract_from_content(content, "test.scala")
        assert len(facts) > 0


class TestShellExtractor:
    def setup_method(self):
        self.extractor = ShellExtractor(create_default_engine())
    
    def test_hdfs_cp(self):
        content = "hdfs dfs -cp /source/path /target/path"
        facts = self.extractor.extract_from_content(content, "test.sh")
        assert len(facts) >= 2  # Read and write
    
    def test_spark_submit(self):
        content = "spark-submit /path/to/job.py"
        facts = self.extractor.extract_from_content(content, "test.sh")
        assert len(facts) > 0

