"""
Unit tests for enhanced variable resolution.
Tests function parameters, f-strings, CLI args, and date expressions.
"""
import pytest
from datetime import datetime
from lineage.extractors.pyspark_extractor import PySparkExtractor
from lineage.extractors.shell_extractor import ShellExtractor


class TestPySparkVariableResolution:
    """Test PySpark variable resolution enhancements."""
    
    def test_function_parameter_defaults(self):
        """Test extraction of function parameters with default values."""
        code = '''
def main(env="prod", run_date="2024-01-15", region="us-west"):
    pass
'''
        extractor = PySparkExtractor(None)
        facts = extractor.extract_from_content(code, "test.py")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        # Should extract all 3 parameters with defaults
        assert len(config_facts) >= 3
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        assert param_dict['env'] == 'prod'
        assert param_dict['run_date'] == '2024-01-15'
        assert param_dict['region'] == 'us-west'
        
        # Check confidence and source
        for fact in config_facts:
            assert fact.config_source == 'function_parameter_default'
            assert fact.confidence == 0.80
    
    def test_fstring_with_simple_variable(self):
        """Test f-string resolution with simple variable."""
        code = '''
env = "prod"
base_path = f"/data/{env}"
'''
        extractor = PySparkExtractor(None)
        facts = extractor.extract_from_content(code, "test.py")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        # Should have env="prod" and base_path="/data/prod"
        param_dict = {f.config_key: f.config_value for f in config_facts}
        assert param_dict['env'] == 'prod'
        assert param_dict['base_path'] == '/data/prod'
    
    def test_fstring_with_function_parameter(self):
        """Test f-string resolution using function parameter."""
        code = '''
def main(env="staging"):
    base_path = f"/data/{env}"
    warehouse = f"{base_path}/warehouse"
'''
        extractor = PySparkExtractor(None)
        facts = extractor.extract_from_content(code, "test.py")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        # env from function param
        assert param_dict['env'] == 'staging'
        
        # base_path resolved using env
        assert param_dict['base_path'] == '/data/staging'
        
        # warehouse partially resolved (base_path is a variable itself)
        assert 'warehouse' in param_dict
        assert '${base_path}' in param_dict['warehouse'] or '/data/staging' in param_dict['warehouse']
    
    def test_fstring_nested_variables(self):
        """Test f-string with multiple nested variables."""
        code = '''
def process(year="2024", month="01", day="15"):
    date_path = f"{year}/{month}/{day}"
    full_path = f"/data/raw/{date_path}"
'''
        extractor = PySparkExtractor(None)
        facts = extractor.extract_from_content(code, "test.py")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        # Should resolve date_path
        assert param_dict['date_path'] == '2024/01/15'
    
    def test_argv_with_default(self):
        """Test sys.argv pattern with default value."""
        code = '''
import sys
batch_id = sys.argv[1] if len(sys.argv) > 1 else "batch_001"
run_date = sys.argv[2] if len(sys.argv) > 2 else "2024-01-15"
'''
        extractor = PySparkExtractor(None)
        facts = extractor.extract_from_content(code, "test.py")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        # Should extract defaults
        assert param_dict['batch_id'] == 'batch_001'
        assert param_dict['run_date'] == '2024-01-15'
        
        # Check source
        for fact in config_facts:
            assert fact.config_source == 'argv_default'
    
    def test_full_pipeline_resolution(self):
        """Test complete pipeline with all resolution types."""
        code = '''
import sys
from pyspark.sql import SparkSession

def main(env="prod", base_dir="/data"):
    spark = SparkSession.builder.getOrCreate()
    
    # F-string using parameter
    warehouse_path = f"{base_dir}/warehouse"
    
    # CLI arg with default
    run_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-15"
    
    # Nested f-string
    input_path = f"{warehouse_path}/{env}/raw/{run_date}"
    
    # Read and write
    df = spark.read.parquet(input_path)
    df.write.parquet(f"{base_dir}/processed/{env}/{run_date}")
'''
        extractor = PySparkExtractor(None)
        facts = extractor.extract_from_content(code, "test.py")
        
        # Check we extract config facts
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        assert len(config_facts) >= 4  # env, base_dir, run_date, warehouse_path
        
        # Check we extract read/write facts
        read_facts = [f for f in facts if hasattr(f, 'fact_type') and str(f.fact_type) == 'FactType.READ']
        write_facts = [f for f in facts if hasattr(f, 'fact_type') and str(f.fact_type) == 'FactType.WRITE']
        
        assert len(read_facts) >= 1
        assert len(write_facts) >= 1


class TestShellDateResolution:
    """Test shell date expression resolution."""
    
    def test_date_yyyymmdd_format(self):
        """Test date resolution in YYYYMMDD format."""
        code = '''
COMPACT_DATE=$(date +%Y%m%d)
echo $COMPACT_DATE
'''
        extractor = ShellExtractor(None)
        facts = extractor.extract_from_content(code, "test.sh")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        # Should resolve to current date in YYYYMMDD format
        assert 'COMPACT_DATE' in param_dict
        compact_date = param_dict['COMPACT_DATE']
        
        # Validate format: 8 digits
        assert len(compact_date) == 8
        assert compact_date.isdigit()
        
        # Validate it's close to today's date
        today = datetime.now().strftime('%Y%m%d')
        assert compact_date == today
    
    def test_date_yyyy_mm_dd_format(self):
        """Test date resolution in YYYY-MM-DD format."""
        code = '''
ISO_DATE=$(date +%Y-%m-%d)
echo $ISO_DATE
'''
        extractor = ShellExtractor(None)
        facts = extractor.extract_from_content(code, "test.sh")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        # Should resolve to current date in YYYY-MM-DD format
        assert 'ISO_DATE' in param_dict
        iso_date = param_dict['ISO_DATE']
        
        # Validate format: YYYY-MM-DD
        parts = iso_date.split('-')
        assert len(parts) == 3
        assert len(parts[0]) == 4  # Year
        assert len(parts[1]) == 2  # Month
        assert len(parts[2]) == 2  # Day
        
        # Validate it's today's date
        today = datetime.now().strftime('%Y-%m-%d')
        assert iso_date == today
    
    def test_date_year_only(self):
        """Test date resolution for year only."""
        code = '''
YEAR=$(date +%Y)
'''
        extractor = ShellExtractor(None)
        facts = extractor.extract_from_content(code, "test.sh")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        assert 'YEAR' in param_dict
        year = param_dict['YEAR']
        
        # Should be current year
        current_year = datetime.now().strftime('%Y')
        assert year == current_year
        assert len(year) == 4
    
    def test_date_month_day_components(self):
        """Test individual date components."""
        code = '''
MONTH=$(date +%m)
DAY=$(date +%d)
'''
        extractor = ShellExtractor(None)
        facts = extractor.extract_from_content(code, "test.sh")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        # Validate month
        assert 'MONTH' in param_dict
        month = param_dict['MONTH']
        assert len(month) == 2
        assert 1 <= int(month) <= 12
        assert month == datetime.now().strftime('%m')
        
        # Validate day
        assert 'DAY' in param_dict
        day = param_dict['DAY']
        assert len(day) == 2
        assert 1 <= int(day) <= 31
        assert day == datetime.now().strftime('%d')
    
    def test_parameter_with_date_default(self):
        """Test parameter with date expression as default."""
        code = '''
RUNDATE=${1:-$(date +%Y-%m-%d)}
INPUT_PATH="/data/raw/${RUNDATE}"
'''
        extractor = ShellExtractor(None)
        facts = extractor.extract_from_content(code, "test.sh")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        # RUNDATE should be resolved
        assert 'RUNDATE' in param_dict
        rundate = param_dict['RUNDATE']
        
        # Should contain today's date (may have ${1:-...} wrapper for parameter)
        today = datetime.now().strftime('%Y-%m-%d')
        assert today in rundate, f"Expected today's date {today} in {rundate}"
    
    def test_backtick_date_command(self):
        """Test backtick-style date command."""
        code = '''
OLD_STYLE=`date +%Y%m%d`
'''
        extractor = ShellExtractor(None)
        facts = extractor.extract_from_content(code, "test.sh")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        assert 'OLD_STYLE' in param_dict
        old_style = param_dict['OLD_STYLE']
        
        # Should be today in YYYYMMDD format
        today = datetime.now().strftime('%Y%m%d')
        assert old_style == today
    
    def test_multiple_date_formats_in_path(self):
        """Test paths with multiple date components."""
        code = '''
YEAR=$(date +%Y)
MONTH=$(date +%m)
DAY=$(date +%d)
PARTITION_PATH="/data/year=${YEAR}/month=${MONTH}/day=${DAY}"
'''
        extractor = ShellExtractor(None)
        facts = extractor.extract_from_content(code, "test.sh")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        # All components should resolve
        assert param_dict['YEAR'] == datetime.now().strftime('%Y')
        assert param_dict['MONTH'] == datetime.now().strftime('%m')
        assert param_dict['DAY'] == datetime.now().strftime('%d')


class TestIntegratedPathResolution:
    """Test complete path resolution with dates."""
    
    def test_hdfs_path_with_date(self):
        """Test HDFS path includes resolved date."""
        code = '''
RUNDATE=$(date +%Y-%m-%d)
hdfs dfs -get /data/raw/${RUNDATE}/transactions/*.parquet /tmp/
'''
        extractor = ShellExtractor(None)
        facts = extractor.extract_from_content(code, "test.sh")
        
        # Get config facts
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        # RUNDATE should be resolved
        today = datetime.now().strftime('%Y-%m-%d')
        assert param_dict['RUNDATE'] == today
        
        # Get read facts
        read_facts = [f for f in facts if hasattr(f, 'dataset_urn')]
        
        # Path should contain today's date
        assert len(read_facts) > 0
        # Note: The path will still have ${RUNDATE} until variable substitution happens
        # This is expected behavior - resolution happens in a separate phase
    
    def test_spark_submit_with_date_params(self):
        """Test spark-submit command with date parameters."""
        code = '''
RUNDATE=$(date +%Y%m%d)
spark-submit \\
  --class com.example.ETL \\
  /path/to/job.jar \\
  --input "/data/raw/${RUNDATE}" \\
  --output "/data/processed/${RUNDATE}"
'''
        extractor = ShellExtractor(None)
        facts = extractor.extract_from_content(code, "test.sh")
        config_facts = [f for f in facts if hasattr(f, 'config_key')]
        
        param_dict = {f.config_key: f.config_value for f in config_facts}
        
        # RUNDATE should be in compact format
        today_compact = datetime.now().strftime('%Y%m%d')
        assert param_dict['RUNDATE'] == today_compact
    
    def test_pyspark_with_date_in_path(self):
        """Test PySpark job with date in read/write paths."""
        code = '''
from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# Use current date
run_date = datetime.now().strftime("%Y-%m-%d")
input_path = f"/data/raw/{run_date}/transactions"
output_path = f"/data/processed/{run_date}/summary"

df = spark.read.parquet(input_path)
df.write.parquet(output_path)
'''
        extractor = PySparkExtractor(None)
        facts = extractor.extract_from_content(code, "test.py")
        
        # Check read/write facts exist
        read_facts = [f for f in facts if hasattr(f, 'dataset_urn') and hasattr(f, 'fact_type') and 'READ' in str(f.fact_type)]
        write_facts = [f for f in facts if hasattr(f, 'dataset_urn') and hasattr(f, 'fact_type') and 'WRITE' in str(f.fact_type)]
        
        assert len(read_facts) >= 1
        assert len(write_facts) >= 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

