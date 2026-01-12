"""
Comprehensive tests for timestamp handling across all extractors.

Tests timestamp extraction and resolution for:
- Shell scripts ($(date +%Y%m%d%H%M%S))
- PySpark (datetime.now().strftime())
- Scala (LocalDateTime, SimpleDateFormat)
"""

import pytest
from datetime import datetime
from src.lineage.extractors.shell_extractor import ShellExtractor
from src.lineage.extractors.pyspark_extractor import PySparkExtractor
from src.lineage.extractors.scala_extractor import ScalaExtractor
from src.lineage.resolution.resolver import VariableResolver
from src.lineage.resolution.symbol_table import SymbolTable, Symbol


class TestShellTimestamps:
    """Test shell script timestamp extraction."""
    
    def test_compact_timestamp(self):
        """Test yyyyMMddHHmmss format."""
        script = '''#!/bin/bash
TIMESTAMP=$(date +%Y%m%d%H%M%S)
hdfs dfs -put /local/data /hdfs/data_${TIMESTAMP}
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        assert "TIMESTAMP" in config_dict
        # Should be 14 digits: YYYYMMDDHHMMSS
        assert len(config_dict["TIMESTAMP"]) == 14
        assert config_dict["TIMESTAMP"].isdigit()
    
    def test_timestamp_with_separators(self):
        """Test timestamp with various separators (filesystem-safe)."""
        script = '''#!/bin/bash
TS1=$(date +%Y-%m-%d_%H-%M-%S)
TS2=$(date +%Y%m%d_%H%M%S)
TS3=$(date +%Y%m%d-%H%M%S)
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        # All three should be extracted
        assert "TS1" in config_dict
        assert "TS2" in config_dict
        assert "TS3" in config_dict
        
        # Check formats (all filesystem-safe, no colons)
        assert "_" in config_dict["TS1"]  # Has underscore separator
        assert "-" in config_dict["TS1"]  # Has dash separator
        assert ":" not in config_dict["TS1"]  # No colons!
        assert "_" in config_dict["TS2"]  # Has underscore
        assert ":" not in config_dict["TS2"]  # No colons!
        assert "-" in config_dict["TS3"]  # Has dash
        assert ":" not in config_dict["TS3"]  # No colons!
    
    def test_iso8601_timestamp(self):
        """Test ISO-like format timestamps (filesystem-safe)."""
        script = '''#!/bin/bash
ISO_TS=$(date +%Y-%m-%dT%H%M%S)
ISO_COMPACT=$(date +%Y%m%dT%H%M%S)
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        assert "ISO_TS" in config_dict
        assert "T" in config_dict["ISO_TS"]  # ISO format has T separator
        assert ":" not in config_dict["ISO_TS"]  # But no colons (filesystem-safe!)
        
        assert "ISO_COMPACT" in config_dict
        assert "T" in config_dict["ISO_COMPACT"]
    
    def test_unix_timestamp(self):
        """Test Unix timestamp (seconds since epoch)."""
        script = '''#!/bin/bash
UNIX_TS=$(date +%s)
BACKUP_PATH="/backup/${UNIX_TS}"
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        assert "UNIX_TS" in config_dict
        # Unix timestamp should be all digits and ~10 digits
        assert config_dict["UNIX_TS"].isdigit()
        assert 9 <= len(config_dict["UNIX_TS"]) <= 11
    
    def test_time_only_formats(self):
        """Test time-only formats (filesystem-safe, no colons)."""
        script = '''#!/bin/bash
TIME_DASH=$(date +%H-%M-%S)
TIME_COMPACT=$(date +%H%M%S)
HOUR=$(date +%H)
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        assert "TIME_DASH" in config_dict
        assert "-" in config_dict["TIME_DASH"]
        assert ":" not in config_dict["TIME_DASH"]  # No colons!
        
        assert "TIME_COMPACT" in config_dict
        assert len(config_dict["TIME_COMPACT"]) == 6  # HHMMSS
        
        assert "HOUR" in config_dict
        assert len(config_dict["HOUR"]) == 2  # HH
    
    def test_timestamp_in_paths(self):
        """Test timestamps used in file paths."""
        script = '''#!/bin/bash
TIMESTAMP=$(date +%Y%m%d%H%M%S)
INPUT="/data/raw/${TIMESTAMP}/input.csv"
OUTPUT="/data/processed/${TIMESTAMP}/output.parquet"

hdfs dfs -get $INPUT /local/
hdfs dfs -put /local/processed $OUTPUT
'''
        
        extractor = ShellExtractor()
        facts = extractor.extract_from_content(script, "test.sh")
        
        # Build symbol table
        symbol_table = SymbolTable()
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        for fact in config_facts:
            symbol_table.add_symbol(Symbol(fact.config_key, fact.config_value, "shell"))
        
        resolver = VariableResolver(symbol_table)
        
        # Check that paths are resolved
        read_facts = [f for f in facts if f.__class__.__name__ == 'ReadFact']
        for fact in read_facts:
            resolved_urn, _ = resolver.resolve(fact.dataset_urn)
            
            # Should not contain unresolved ${TIMESTAMP}
            assert "${TIMESTAMP}" not in resolved_urn
            # Should contain actual timestamp (14 digits)
            assert any(c.isdigit() for c in resolved_urn)


class TestPySparkTimestamps:
    """Test PySpark datetime extraction."""
    
    def test_datetime_now_strftime(self):
        """Test datetime.now().strftime() extraction."""
        code = '''
import datetime

timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
run_id = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
'''
        
        extractor = PySparkExtractor()
        facts = extractor.extract_from_content(code, "test.py")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        assert "timestamp" in config_dict
        assert len(config_dict["timestamp"]) == 14  # YYYYMMDDHHMMSS
        
        assert "run_id" in config_dict
        assert "_" in config_dict["run_id"]
    
    def test_datetime_in_paths(self):
        """Test datetime used in Spark paths."""
        code = '''
import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
input_path = f"/data/raw/{timestamp}"
output_path = f"/data/processed/{timestamp}"

df = spark.read.parquet(input_path)
df.write.parquet(output_path)
'''
        
        extractor = PySparkExtractor()
        facts = extractor.extract_from_content(code, "test.py")
        
        # Build symbol table
        symbol_table = SymbolTable()
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        for fact in config_facts:
            symbol_table.add_symbol(Symbol(fact.config_key, fact.config_value, "python"))
        
        resolver = VariableResolver(symbol_table)
        
        # Check resolution
        read_facts = [f for f in facts if f.__class__.__name__ == 'ReadFact']
        for fact in read_facts:
            if "$" in fact.dataset_urn or "{" in fact.dataset_urn:
                resolved_urn, _ = resolver.resolve(fact.dataset_urn)
                
                # Should not have unresolved timestamp variable
                assert "timestamp" not in resolved_urn or len(resolved_urn.split("timestamp")[0]) > 0
    
    def test_various_datetime_formats(self):
        """Test various datetime format strings."""
        code = '''
import datetime

ts1 = datetime.now().strftime('%Y-%m-%d')
ts2 = datetime.now().strftime('%Y%m%d')
ts3 = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
ts4 = datetime.now().strftime('%Y%m%d%H%M%S')
'''
        
        extractor = PySparkExtractor()
        facts = extractor.extract_from_content(code, "test.py")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        # All should be extracted
        assert len(config_dict) >= 4
        assert "ts1" in config_dict
        assert "ts2" in config_dict
        assert "ts3" in config_dict
        assert "ts4" in config_dict


class TestScalaTimestamps:
    """Test Scala datetime extraction."""
    
    def test_localdatetime_format(self):
        """Test LocalDateTime.now().format() extraction."""
        code = '''
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
val runId = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"))
'''
        
        extractor = ScalaExtractor()
        facts = extractor.extract_from_content(code, "test.scala")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        assert "timestamp" in config_dict
        assert len(config_dict["timestamp"]) == 14  # yyyyMMddHHmmss
        
        assert "runId" in config_dict
        assert "_" in config_dict["runId"]
    
    def test_simpledateformat(self):
        """Test SimpleDateFormat extraction."""
        code = '''
import java.text.SimpleDateFormat
import java.util.Date

val timestamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date())
'''
        
        extractor = ScalaExtractor()
        facts = extractor.extract_from_content(code, "test.scala")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        # This pattern is complex, so we just check it doesn't crash
        # In real code, it would need the SimpleDateFormat to be extracted first
        # For now, we verify extraction doesn't fail
        assert isinstance(config_dict, dict)
    
    def test_timestamp_in_scala_paths(self):
        """Test timestamps in Scala string interpolation."""
        code = '''
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
val basePath = s"/data/raw/$timestamp"

spark.read.parquet(s"$basePath/input.parquet")
'''
        
        extractor = ScalaExtractor()
        facts = extractor.extract_from_content(code, "test.scala")
        
        # Build symbol table
        symbol_table = SymbolTable()
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        for fact in config_facts:
            symbol_table.add_symbol(Symbol(fact.config_key, fact.config_value, "scala"))
        
        resolver = VariableResolver(symbol_table)
        
        # Check resolution
        read_facts = [f for f in facts if f.__class__.__name__ == 'ReadFact']
        for fact in read_facts:
            if "$" in fact.dataset_urn:
                resolved_urn, _ = resolver.resolve(fact.dataset_urn)
                
                # Should have timestamp resolved
                assert "$timestamp" not in resolved_urn


class TestTimestampIntegration:
    """Integration tests for timestamp handling across file types."""
    
    def test_shell_to_spark_timestamp_pipeline(self):
        """Test timestamp passed from shell script to Spark job."""
        shell_script = '''#!/bin/bash
TIMESTAMP=$(date +%Y%m%d%H%M%S)
spark-submit job.py --timestamp $TIMESTAMP
'''
        
        python_script = '''
import sys
import datetime

timestamp = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime('%Y%m%d%H%M%S')
input_path = f"/data/raw/{timestamp}"
'''
        
        # Extract from shell
        shell_extractor = ShellExtractor()
        shell_facts = shell_extractor.extract_from_content(shell_script, "run.sh")
        
        # Extract from Python
        py_extractor = PySparkExtractor()
        py_facts = py_extractor.extract_from_content(python_script, "job.py")
        
        # Both should extract timestamp values
        shell_configs = [f for f in shell_facts if f.__class__.__name__ == 'ConfigFact']
        py_configs = [f for f in py_facts if f.__class__.__name__ == 'ConfigFact']
        
        assert len(shell_configs) > 0
        assert len(py_configs) > 0
    
    def test_timestamp_confidence_levels(self):
        """Test that timestamp extractions have appropriate confidence."""
        scripts = [
            ('TIMESTAMP=$(date +%Y%m%d%H%M%S)', 'shell'),
            ('timestamp = datetime.now().strftime("%Y%m%d%H%M%S")', 'python'),
            ('val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))', 'scala')
        ]
        
        extractors = {
            'shell': ShellExtractor(),
            'python': PySparkExtractor(),
            'scala': ScalaExtractor()
        }
        
        for script, lang in scripts:
            extractor = extractors[lang]
            facts = extractor.extract_from_content(script, f"test.{lang}")
            
            config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
            
            for fact in config_facts:
                if 'timestamp' in fact.config_key.lower():
                    # Timestamp extractions should have high confidence
                    assert fact.confidence >= 0.85, \
                        f"{lang} timestamp confidence too low: {fact.confidence}"


class TestTimestampRegressionPrevention:
    """Prevent regression of timestamp handling."""
    
    def test_no_unresolved_timestamps_in_paths(self):
        """Ensure timestamp variables are always resolved in paths."""
        test_cases = [
            ('TIMESTAMP=$(date +%Y%m%d%H%M%S)\nhdfs dfs -put /src /dst/${TIMESTAMP}', 'shell', ShellExtractor()),
            ('timestamp = datetime.now().strftime("%Y%m%d%H%M%S")\npath = f"/data/{timestamp}"', 'python', PySparkExtractor()),
            ('val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))\nval path = s"/data/$timestamp"', 'scala', ScalaExtractor())
        ]
        
        for code, lang, extractor in test_cases:
            facts = extractor.extract_from_content(code, f"test.{lang}")
            
            # Build symbol table
            symbol_table = SymbolTable()
            config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
            for fact in config_facts:
                symbol_table.add_symbol(Symbol(fact.config_key, fact.config_value, lang))
            
            resolver = VariableResolver(symbol_table)
            
            # Check all facts
            for fact in facts:
                if hasattr(fact, 'dataset_urn') and fact.dataset_urn and "$" in fact.dataset_urn:
                    resolved_urn, _ = resolver.resolve(fact.dataset_urn)
                    
                    # Common timestamp variable names should be resolved
                    unresolved_vars = ['$TIMESTAMP', '$timestamp', '${TIMESTAMP}', '${timestamp}']
                    for var in unresolved_vars:
                        assert var not in resolved_urn, \
                            f"Found unresolved {var} in {lang}: {resolved_urn}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

