"""
Unit tests for resolution fixes.

Tests all the fixes for resolution issues:
1. PySpark os.getenv() defaults extraction
2. Hive lowercase hiveconf preservation
3. Shell date arithmetic $(date -d ...)
4. Shell -delete flag filtering
5. Hive variable resolution ${hiveconf:...}
"""

import pytest
from pathlib import Path
from lineage.extractors.pyspark_extractor import PySparkExtractor
from lineage.extractors.hive_extractor import HiveExtractor
from lineage.extractors.shell_extractor import ShellExtractor
from lineage.resolution.resolver import VariableResolver
from lineage.resolution.symbol_table import SymbolTable, Symbol


class TestPySparkOsGetenvExtraction:
    """Test PySpark os.getenv() default value extraction."""
    
    def test_os_getenv_with_default(self):
        """Test extraction of os.getenv() with default value."""
        code = '''
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Test").getOrCreate()

# These should be extracted
input_db = os.getenv("INPUT_DATABASE", "prod")
output_db = os.getenv("OUTPUT_DATABASE", "analytics")
env = os.getenv("ENVIRONMENT", "production")

# Read and write using the variables
df = spark.table(f"{input_db}.transactions")
df.write.saveAsTable(f"{output_db}.daily_summary")
'''
        
        extractor = PySparkExtractor()
        facts = extractor.extract_from_content(code, "test.py")
        
        # Find ConfigFacts
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        
        # Should have extracted all three os.getenv() defaults
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        assert "input_db" in config_dict, "input_db not extracted"
        assert config_dict["input_db"] == "prod", f"Expected 'prod', got '{config_dict['input_db']}'"
        
        assert "output_db" in config_dict, "output_db not extracted"
        assert config_dict["output_db"] == "analytics", f"Expected 'analytics', got '{config_dict['output_db']}'"
        
        assert "env" in config_dict, "env not extracted"
        assert config_dict["env"] == "production", f"Expected 'production', got '{config_dict['env']}'"
    
    def test_os_environ_get_with_default(self):
        """Test extraction of os.environ.get() with default value."""
        code = '''
import os

# Alternative syntax
db_name = os.environ.get("DB_NAME", "default_db")
table_name = os.environ.get("TABLE_NAME", "default_table")
'''
        
        extractor = PySparkExtractor()
        facts = extractor.extract_from_content(code, "test.py")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        assert config_dict.get("db_name") == "default_db"
        assert config_dict.get("table_name") == "default_table"
    
    def test_os_getenv_without_default(self):
        """Test that os.getenv() without default is not extracted as ConfigFact."""
        code = '''
import os

# No default - should not create ConfigFact
input_path = os.getenv("INPUT_PATH")
'''
        
        extractor = PySparkExtractor()
        facts = extractor.extract_from_content(code, "test.py")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        # Should not extract input_path as ConfigFact since no default
        assert "input_path" not in config_dict or config_dict["input_path"] is None


class TestHiveLowercasePreservation:
    """Test Hive variable case preservation."""
    
    def test_lowercase_hiveconf_in_create_table(self):
        """Test that ${hiveconf:...} stays lowercase in CREATE TABLE."""
        sql = '''
-- Test lowercase preservation
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:analytics_db}.fraud_patterns (
    customer_id STRING,
    pattern_type STRING
)
LOCATION '/data/warehouse/${hiveconf:env}/analytics/fraud_patterns';
'''
        
        extractor = HiveExtractor()
        facts = extractor.extract_from_content(sql, "test.hql")
        
        # Find write facts (CREATE TABLE writes to table)
        write_facts = [f for f in facts if f.__class__.__name__ == 'WriteFact' 
                       and f.dataset_type == 'hive']
        
        assert len(write_facts) > 0, "No write facts found"
        
        # Check that the table name preserved lowercase
        table_urn = write_facts[0].dataset_urn
        assert "${hiveconf:analytics_db}" in table_urn or "hiveconf:analytics_db" in table_urn, \
            f"Lowercase not preserved in table URN: {table_urn}"
        assert "${HIVECONF:" not in table_urn, f"Found uppercase HIVECONF in: {table_urn}"
    
    def test_lowercase_hiveconf_in_insert(self):
        """Test that ${hiveconf:...} stays lowercase in INSERT."""
        sql = '''
INSERT OVERWRITE TABLE ${hiveconf:reports_db}.customer_export
SELECT * FROM ${hiveconf:analytics_db}.customers;
'''
        
        extractor = HiveExtractor()
        facts = extractor.extract_from_content(sql, "test.hql")
        
        write_facts = [f for f in facts if f.__class__.__name__ == 'WriteFact']
        
        assert len(write_facts) > 0, "No write facts found"
        
        table_urn = write_facts[0].dataset_urn
        # Should preserve lowercase hiveconf
        if "${" in table_urn:  # If not resolved
            assert "${HIVECONF:" not in table_urn, f"Found uppercase HIVECONF in: {table_urn}"
            # Check it's lowercase (either hiveconf or HIVECONF)
            assert "hiveconf" in table_urn.lower()
    
    def test_mixed_case_hiveconf(self):
        """Test handling of mixed case hiveconf."""
        sql = '''
-- Mix of lowercase and uppercase (user might write either)
CREATE TABLE ${hiveconf:db1}.table1 AS SELECT * FROM ${HIVECONF:db2}.table2;
'''
        
        extractor = HiveExtractor()
        facts = extractor.extract_from_content(sql, "test.hql")
        
        # Both should be extracted, case preserved
        write_facts = [f for f in facts if f.__class__.__name__ == 'WriteFact']
        read_facts = [f for f in facts if f.__class__.__name__ == 'ReadFact']
        
        # Should have both, with original case
        assert len(write_facts) > 0
        assert len(read_facts) > 0


class TestShellDateArithmetic:
    """Test shell date arithmetic handling."""
    
    def test_date_arithmetic_extraction(self):
        """Test that $(date -d ...) is properly extracted."""
        script = '''#!/bin/bash
RUN_DATE=${1:-2024-01-15}
ENV=${2:-prod}

# Date arithmetic - should be resolved to placeholder
ARCHIVE_DATE=$(date -d "${RUN_DATE} -30 days" +%Y-%m-%d)
COMPACT_DATE=$(date -d "yesterday" +%Y%m%d)
MONTH_START=$(date -d "${RUN_DATE} -1 month" +%Y-%m)

# Use in paths
hdfs dfs -mv /data/processed/${ARCHIVE_DATE} /archive/${ARCHIVE_DATE}
hdfs dfs -cp /data/raw/${COMPACT_DATE} /data/staging/${COMPACT_DATE}
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        # Find ConfigFacts for date variables
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        # ARCHIVE_DATE should be resolved to placeholder (not truncated!)
        assert "ARCHIVE_DATE" in config_dict, "ARCHIVE_DATE not extracted"
        archive_value = config_dict["ARCHIVE_DATE"]
        
        # Should NOT be "$(date -d" (truncated)
        assert archive_value != "$(date -d", f"ARCHIVE_DATE truncated: {archive_value}"
        
        # Should be resolved to a placeholder like "YYYY-MM-DD"
        assert "YYYY" in archive_value or "DATE" in archive_value or "-" in archive_value, \
            f"ARCHIVE_DATE not resolved to placeholder: {archive_value}"
    
    def test_date_arithmetic_with_quotes(self):
        """Test date commands with quotes and variables inside."""
        script = '''#!/bin/bash
BASE_DATE="2024-01-15"

# Complex date arithmetic with quotes
PREV_MONTH=$(date -d "${BASE_DATE} -1 month" +%Y-%m-%d)
NEXT_WEEK=$(date -d "$BASE_DATE +7 days" +%Y%m%d)
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        # Should extract both
        assert "PREV_MONTH" in config_dict
        assert "NEXT_WEEK" in config_dict
        
        # Should not be truncated
        assert "$(date" not in config_dict["PREV_MONTH"]
        assert "$(date" not in config_dict["NEXT_WEEK"]


class TestShellDeleteFlagFiltering:
    """Test shell command flag filtering."""
    
    def test_distcp_with_delete_flag(self):
        """Test that -delete flag is not extracted as a dataset."""
        script = '''#!/bin/bash
# hadoop distcp with flags
hadoop distcp -update -delete /source/path /dest/path
hadoop distcp -overwrite -delete -update /src2 /dst2
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        # Get all dataset URNs
        read_facts = [f for f in facts if f.__class__.__name__ == 'ReadFact']
        write_facts = [f for f in facts if f.__class__.__name__ == 'WriteFact']
        
        all_urns = [f.dataset_urn for f in read_facts + write_facts]
        
        # Should NOT have -delete or -update as URNs
        for urn in all_urns:
            assert "-delete" != urn, f"-delete extracted as dataset!"
            assert "-update" != urn, f"-update extracted as dataset!"
            assert "-overwrite" != urn, f"-overwrite extracted as dataset!"
        
        # Should have extracted actual paths
        assert any("/source/path" in urn for urn in all_urns), "Source path not extracted"
        assert any("/dest/path" in urn for urn in all_urns), "Dest path not extracted"
    
    def test_distcp_without_flags(self):
        """Test distcp without flags still works."""
        script = '''#!/bin/bash
hadoop distcp /src /dst
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        read_facts = [f for f in facts if f.__class__.__name__ == 'ReadFact']
        write_facts = [f for f in facts if f.__class__.__name__ == 'WriteFact']
        
        all_urns = [f.dataset_urn for f in read_facts + write_facts]
        
        assert any("/src" in urn for urn in all_urns)
        assert any("/dst" in urn for urn in all_urns)


class TestHiveVariableResolution:
    """Test Hive variable resolution in VariableResolver."""
    
    def test_hiveconf_resolution(self):
        """Test that ${hiveconf:var} resolves correctly."""
        symbol_table = SymbolTable()
        symbol_table.add_symbol(Symbol("analytics_db", "analytics_prod", "config"))
        symbol_table.add_symbol(Symbol("env", "prod", "config"))
        
        resolver = VariableResolver(symbol_table)
        
        # Test hiveconf resolution
        text = "/data/warehouse/${hiveconf:env}/analytics"
        resolved, fully_resolved = resolver.resolve(text)
        
        assert "/data/warehouse/prod/analytics" == resolved, \
            f"Expected resolved path, got: {resolved}"
        assert fully_resolved, "Should be fully resolved"
    
    def test_hiveconf_table_name(self):
        """Test hiveconf in table names."""
        symbol_table = SymbolTable()
        symbol_table.add_symbol(Symbol("reports_db", "reports_prod", "config"))
        symbol_table.add_symbol(Symbol("run_date", "2024-01-15", "config"))
        
        resolver = VariableResolver(symbol_table)
        
        text = "${hiveconf:reports_db}.customer_export_${hiveconf:run_date}"
        resolved, fully_resolved = resolver.resolve(text)
        
        assert resolved == "reports_prod.customer_export_2024-01-15", \
            f"Expected fully resolved table, got: {resolved}"
    
    def test_hivevar_resolution(self):
        """Test ${hivevar:...} syntax."""
        symbol_table = SymbolTable()
        symbol_table.add_symbol(Symbol("db_name", "my_database", "config"))
        
        resolver = VariableResolver(symbol_table)
        
        text = "USE ${hivevar:db_name};"
        resolved, _ = resolver.resolve(text)
        
        assert "my_database" in resolved
    
    def test_mixed_hiveconf_and_regular(self):
        """Test mix of hiveconf and regular variables."""
        symbol_table = SymbolTable()
        symbol_table.add_symbol(Symbol("DB", "prod_db", "config"))
        symbol_table.add_symbol(Symbol("table", "transactions", "config"))
        
        resolver = VariableResolver(symbol_table)
        
        text = "${hiveconf:DB}.${table}"
        resolved, _ = resolver.resolve(text)
        
        assert resolved == "prod_db.transactions"


class TestIntegratedResolution:
    """Integration tests combining extraction and resolution."""
    
    def test_pyspark_osgetenv_full_pipeline(self):
        """Test full pipeline: extract os.getenv() -> resolve in paths."""
        code = '''
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

input_db = os.getenv("INPUT_DB", "bronze")
output_db = os.getenv("OUTPUT_DB", "silver")

df = spark.table(f"{input_db}.raw_data")
df.write.saveAsTable(f"{output_db}.cleaned_data")
'''
        
        extractor = PySparkExtractor()
        facts = extractor.extract_from_content(code, "test.py")
        
        # Build symbol table
        symbol_table = SymbolTable()
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        for fact in config_facts:
            symbol_table.add_symbol(Symbol(fact.config_key, fact.config_value, "python"))
        
        # Try to resolve placeholder table names
        resolver = VariableResolver(symbol_table)
        
        read_facts = [f for f in facts if f.__class__.__name__ == 'ReadFact']
        
        # Find the table read
        table_read = next((f for f in read_facts if f.dataset_type == 'hive'), None)
        assert table_read is not None
        
        # The URN might be like "hive://${input_db}.raw_data" or similar
        # After resolution, should become "bronze.raw_data"
        resolved_urn, _ = resolver.resolve(table_read.dataset_urn)
        
        assert "bronze" in resolved_urn.lower() or "input_db" not in resolved_urn, \
            f"Failed to resolve input_db in: {resolved_urn}"
    
    def test_shell_date_arithmetic_full_pipeline(self):
        """Test full pipeline: extract date arithmetic -> use in paths."""
        script = '''#!/bin/bash
RUN_DATE=${1:-2024-01-15}
ARCHIVE_DATE=$(date -d "${RUN_DATE} -30 days" +%Y-%m-%d)

hdfs dfs -mv /data/processed/${ARCHIVE_DATE} /archive/${ARCHIVE_DATE}
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        # Build symbol table
        symbol_table = SymbolTable()
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        for fact in config_facts:
            symbol_table.add_symbol(Symbol(fact.config_key, fact.config_value, "shell"))
        
        resolver = VariableResolver(symbol_table)
        
        # Find hdfs facts
        read_facts = [f for f in facts if f.__class__.__name__ == 'ReadFact']
        
        for fact in read_facts:
            resolved_urn, _ = resolver.resolve(fact.dataset_urn)
            
            # Should NOT contain "$(date -d"
            assert "$(date -d" not in resolved_urn, \
                f"Date command not resolved in: {resolved_urn}"
            
            # Should have ARCHIVE_DATE resolved to something
            if "ARCHIVE_DATE" in fact.dataset_urn:
                assert "ARCHIVE_DATE" not in resolved_urn or "YYYY" in resolved_urn, \
                    f"ARCHIVE_DATE not resolved in: {resolved_urn}"


class TestRegressionPrevention:
    """Tests to prevent regression of fixed issues."""
    
    def test_no_truncated_date_commands(self):
        """Ensure date commands are never truncated to $(date -d."""
        script = '''#!/bin/bash
DATE1=$(date -d "2024-01-15 -7 days" +%Y-%m-%d)
DATE2=$(date -d "$BASE_DATE +1 month" +%Y%m%d)
DATE3=$(date -d "${RUN_DATE} -30 days" +%Y-%m)
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        
        for fact in config_facts:
            # None should be truncated to "$(date -d"
            assert fact.config_value != "$(date -d", \
                f"Variable {fact.config_key} truncated to '$(date -d'"
    
    def test_no_uppercase_hiveconf(self):
        """Ensure hiveconf variables never get uppercased."""
        sql = '''
CREATE TABLE ${hiveconf:my_db}.my_table AS 
SELECT * FROM ${hiveconf:source_db}.source_table;
'''
        
        extractor = HiveExtractor()
        facts = extractor.extract_from_content(sql, "test.hql")
        
        for fact in facts:
            if hasattr(fact, 'dataset_urn') and fact.dataset_urn:
                # Should NOT have uppercase HIVECONF
                assert "${HIVECONF:" not in fact.dataset_urn, \
                    f"Found uppercase HIVECONF in: {fact.dataset_urn}"
    
    def test_no_flags_as_datasets(self):
        """Ensure command flags are never extracted as datasets."""
        script = '''#!/bin/bash
hadoop distcp -update -delete -overwrite /src /dst
hdfs dfs -rm -r -skipTrash /temp
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        all_urns = []
        for fact in facts:
            if hasattr(fact, 'dataset_urn') and fact.dataset_urn:
                all_urns.append(fact.dataset_urn)
        
        # Check no flags are extracted
        flags = ['-update', '-delete', '-overwrite', '-r', '-skipTrash', '-rm']
        for flag in flags:
            for urn in all_urns:
                assert urn != flag and not urn.startswith(flag + ' '), \
                    f"Flag {flag} extracted as dataset: {urn}"
    
    def test_os_getenv_confidence_level(self):
        """Ensure os.getenv() extraction has appropriate confidence."""
        code = 'db = os.getenv("DB", "default_db")'
        
        extractor = PySparkExtractor()
        facts = extractor.extract_from_content(code, "test.py")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        
        for fact in config_facts:
            if fact.config_key == "db":
                # Should have high confidence (0.85)
                assert fact.confidence >= 0.80, \
                    f"os.getenv() confidence too low: {fact.confidence}"
                assert fact.config_source == "os_getenv_default"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

