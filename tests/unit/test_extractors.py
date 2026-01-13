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
    
    def test_jdbc_read_with_sql_query(self):
        """Test JDBC read with SQL query in parentheses extracts table names correctly."""
        content = '''
val dfCustomers = spark.read
  .jdbc(
    s"jdbc:oracle:thin:@${config.oracleHost}:1521:ORCL",
    "(SELECT * FROM CUSTOMER_MASTER WHERE UPDATED_DATE >= TO_DATE(?, 'YYYY-MM-DD'))",
    oracleProps
  )
'''
        facts = self.extractor.extract_from_content(content, "test.scala")
        
        # Should extract READ fact with table name, not SQL query
        read_facts = [f for f in facts if f.fact_type == FactType.READ and hasattr(f, 'dataset_type') and f.dataset_type == 'jdbc']
        assert len(read_facts) > 0, "Should extract JDBC read facts"
        
        # Check that we got the table name, not the SQL query
        jdbc_urns = [f.dataset_urn for f in read_facts]
        assert any('CUSTOMER_MASTER' in urn for urn in jdbc_urns), "Should extract table name CUSTOMER_MASTER"
        
        # Ensure no SQL fragments in dataset URNs
        for urn in jdbc_urns:
            assert 'SELECT' not in urn, f"Dataset URN should not contain SQL fragments: {urn}"
            assert 'WHERE' not in urn, f"Dataset URN should not contain SQL fragments: {urn}"
    
    def test_jdbc_read_with_join_query(self):
        """Test JDBC read with JOIN query extracts multiple table names."""
        content = '''
val df = spark.read
  .jdbc(
    postgresUrl, 
    """(SELECT 
      t.transaction_id,
      t.customer_id,
      p.product_name
    FROM transactions.fact_transactions t
    JOIN products.dim_products p ON t.product_id = p.product_id
    WHERE t.transaction_date >= ?)""",
    connectionProps
  )
'''
        facts = self.extractor.extract_from_content(content, "test.scala")
        
        # Should extract READ facts for both tables
        read_facts = [f for f in facts if f.fact_type == FactType.READ and hasattr(f, 'dataset_type') and f.dataset_type == 'jdbc']
        assert len(read_facts) >= 2, "Should extract JDBC read facts for both tables"
        
        # Check that we got both table names
        jdbc_urns = [f.dataset_urn for f in read_facts]
        assert any('transactions.fact_transactions' in urn for urn in jdbc_urns), "Should extract transactions.fact_transactions"
        assert any('products.dim_products' in urn for urn in jdbc_urns), "Should extract products.dim_products"
    
    def test_jdbc_read_simple_table(self):
        """Test JDBC read with simple table name still works."""
        content = '''
val df = spark.read
  .jdbc(url, "customers", connectionProps)
'''
        facts = self.extractor.extract_from_content(content, "test.scala")
        
        # Should extract READ fact with simple table name
        read_facts = [f for f in facts if f.fact_type == FactType.READ and hasattr(f, 'dataset_type') and f.dataset_type == 'jdbc']
        assert len(read_facts) > 0, "Should extract JDBC read facts"
        
        # Check that we got the table name
        jdbc_urns = [f.dataset_urn for f in read_facts]
        assert any('customers' in urn for urn in jdbc_urns), "Should extract table name customers"



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

