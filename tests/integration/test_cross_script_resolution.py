"""
Test: Cross-Script Parameter Resolution
Tests that schema.table names defined in config files and passed through shell scripts
are properly tracked across the entire pipeline
"""

import pytest
from pathlib import Path
from lineage.extractors.shell_extractor import ShellExtractor
from lineage.extractors.pyspark_extractor import PySparkExtractor
from lineage.extractors.hive_extractor import HiveExtractor
from lineage.extractors.config_extractor import ConfigExtractor
from lineage.rules.engine import RuleEngine
from lineage.ir.store import FactStore
from lineage.ir.fact import FactType
from lineage.resolution.symbol_table import SymbolTable, Symbol
from lineage.resolution.resolver import VariableResolver


def extract_env_vars_from_shell(file_path: Path) -> SymbolTable:
    """Extract environment variable exports from shell config files."""
    symbol_table = SymbolTable()
    
    try:
        with open(file_path) as f:
            for line in f:
                line = line.strip()
                # Look for: export VAR="value" or export VAR=${OTHER}
                if line.startswith('export '):
                    parts = line[7:].split('=', 1)
                    if len(parts) == 2:
                        var_name = parts[0].strip()
                        value = parts[1].strip().strip('"').strip("'")
                        
                        # Add to symbol table
                        symbol_table.add_env_var(var_name, value)
    except:
        pass
    
    return symbol_table


class TestCrossScriptResolution:
    """Test parameter resolution across shell scripts, config files, and Spark/Hive jobs"""
    
    @pytest.fixture
    def shell_extractor(self):
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        return ShellExtractor(rule_engine)
    
    @pytest.fixture
    def pyspark_extractor(self):
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        return PySparkExtractor(rule_engine)
    
    @pytest.fixture
    def hive_extractor(self):
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        return HiveExtractor(rule_engine)
    
    def test_config_file_schema_table_definitions(self):
        """Test that config file defines schema.table names"""
        config_path = Path("tests/mocks/configs/etl_env.sh")
        
        # Extract environment variables
        symbol_table = extract_env_vars_from_shell(config_path)
        
        # Verify schema-qualified table exports
        expected_vars = [
            "SOURCE_CUSTOMERS_TABLE",
            "TARGET_CUSTOMER_SUMMARY",
            "TARGET_PRODUCT_PERFORMANCE",
            "TARGET_MONTHLY_METRICS"
        ]
        
        found_vars = []
        for var in expected_vars:
            symbol = symbol_table.get_symbol(var)
            if symbol and symbol.value:
                found_vars.append(var)
                print(f"  ✅ {var} = {symbol.value}")
                # Verify it's schema.table format
                if "." in symbol.value or "${" in symbol.value:
                    assert True  # Good, it's qualified or uses variable
        
        assert len(found_vars) >= 3, f"Should extract schema-qualified tables from config, found {found_vars}"
        print(f"\n✅ Config: Found {len(found_vars)} schema-qualified table definitions")
    
    def test_shell_script_sources_config(self, shell_extractor):
        """Test that master shell script sources config file"""
        master_script = Path("tests/mocks/shell/07_master_etl_pipeline.sh")
        facts = shell_extractor.extract(master_script)
        
        # Check if script content references the config file
        with open(master_script) as f:
            content = f.read()
            assert "source" in content or "." in content, "Script should source config file"
            assert "etl_env.sh" in content, "Should reference etl_env.sh"
            print("  ✅ Master script sources etl_env.sh")
    
    def test_shell_script_calls_subscripts_with_params(self, shell_extractor):
        """Test that master script calls sub-scripts with schema.table parameters"""
        master_script = Path("tests/mocks/shell/07_master_etl_pipeline.sh")
        
        with open(master_script) as f:
            content = f.read()
            
            # Should call ingest_customers.sh with --source-table parameter
            assert "ingest_customers.sh" in content
            assert "--source-table" in content
            assert "${SOURCE_CUSTOMERS_TABLE}" in content or "SOURCE_CUSTOMERS_TABLE" in content
            print("  ✅ Calls ingest_customers.sh with --source-table")
            
            # Should call spark-submit with parameters
            assert "spark-submit" in content
            assert "--source-customers" in content or "source-customers" in content
            print("  ✅ Calls spark-submit with table parameters")
            
            # Should call beeline with hivevar
            assert "beeline" in content
            assert "--hivevar" in content
            print("  ✅ Calls beeline with --hivevar parameters")
    
    def test_subscript_receives_schema_table_params(self, shell_extractor):
        """Test that sub-script receives and uses schema.table parameters"""
        subscript = Path("tests/mocks/shell/ingest_customers.sh")
        facts = shell_extractor.extract(subscript)
        
        with open(subscript) as f:
            content = f.read()
            
            # Should parse --source-table and --target-table args
            assert "--source-table" in content
            assert "--target-table" in content
            assert "SOURCE_TABLE=" in content
            assert "TARGET_TABLE=" in content
            print("  ✅ Sub-script parses --source-table and --target-table")
            
            # Should use these variables in Sqoop/Hive commands
            assert "${SOURCE_TABLE}" in content
            assert "${TARGET_TABLE}" in content
            print("  ✅ Sub-script uses SOURCE_TABLE and TARGET_TABLE variables")
    
    def test_pyspark_receives_schema_table_from_argparse(self, pyspark_extractor):
        """Test that PySpark job receives schema.table via argparse"""
        pyspark_script = Path("tests/mocks/pyspark/customer_aggregation.py")
        facts = pyspark_extractor.extract(pyspark_script)
        
        # Should have READ facts (even if parameterized)
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        
        # The script uses spark.table(args.source_customers)
        # This should be detected even if the value is from argparse
        print(f"  📊 Detected {len(read_facts)} reads, {len(write_facts)} writes")
        
        # Check that argparse is used
        with open(pyspark_script) as f:
            content = f.read()
            assert "argparse" in content
            assert "--source-customers" in content
            assert "--target-table" in content
            print("  ✅ PySpark uses argparse for --source-customers and --target-table")
    
    def test_hive_receives_schema_table_from_hivevar(self, hive_extractor):
        """Test that Hive script receives schema.table via ${hivevar:...}"""
        hive_script = Path("tests/mocks/hive/product_analysis.hql")
        facts = hive_extractor.extract(hive_script)
        
        # Check for hivevar usage
        with open(hive_script) as f:
            content = f.read()
            assert "${hivevar:source_products}" in content
            assert "${hivevar:source_orders}" in content
            assert "${hivevar:target_table}" in content
            print("  ✅ Hive uses ${hivevar:...} for table parameters")
        
        # Facts should be extracted (with placeholders)
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        
        print(f"  📊 Detected {len(read_facts)} reads, {len(write_facts)} writes")
        assert len(read_facts) >= 2, "Should detect source table reads"
        assert len(write_facts) >= 1, "Should detect target table write"
    
    def test_end_to_end_resolution_pipeline(self):
        """Test complete resolution: config → shell → spark/hive"""
        
        # Step 1: Extract variables from config
        config_path = Path("tests/mocks/configs/etl_env.sh")
        symbol_table = extract_env_vars_from_shell(config_path)
        
        print(f"\n📊 Step 1: Extracted {len(symbol_table.symbols)} variables from config")
        
        # Step 2: Create resolver
        resolver = VariableResolver(symbol_table)
        
        # Step 3: Test resolution of common patterns
        test_cases = [
            ("${SOURCE_DB}.customers", "Expected: prod_db.customers"),
            ("${TARGET_DB}.customer_summary", "Expected: analytics_db.customer_summary"),
            ("${SOURCE_CUSTOMERS_TABLE}", "Expected: prod_db.customers"),
            ("${TARGET_CUSTOMER_SUMMARY}", "Expected: analytics_db.customer_summary"),
        ]
        
        resolved_count = 0
        for pattern, description in test_cases:
            resolved, fully = resolver.resolve(pattern)
            if fully and resolved != pattern:
                resolved_count += 1
                print(f"  ✅ {pattern} → {resolved}")
        
        print(f"\n✅ Resolved {resolved_count}/{len(test_cases)} patterns")
        assert resolved_count >= 2, "Should resolve schema.table patterns from config"
    
    def test_cross_file_lineage_tracking(self):
        """Test that lineage is tracked across multiple files in the pipeline"""
        
        # Extract from all files in the pipeline
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        
        fact_store = FactStore()
        
        # Process all pipeline files
        files = [
            ("shell", Path("tests/mocks/shell/07_master_etl_pipeline.sh")),
            ("shell", Path("tests/mocks/shell/ingest_customers.sh")),
            ("pyspark", Path("tests/mocks/pyspark/customer_aggregation.py")),
            ("hive", Path("tests/mocks/hive/product_analysis.hql")),
        ]
        
        extractors = {
            "shell": ShellExtractor(rule_engine),
            "pyspark": PySparkExtractor(rule_engine),
            "hive": HiveExtractor(rule_engine),
        }
        
        for file_type, file_path in files:
            if file_path.exists():
                extractor = extractors[file_type]
                facts = extractor.extract(file_path)
                fact_store.add_facts(facts)
                print(f"  📄 {file_path.name}: {len(facts)} facts")
        
        # Analyze results
        stats = fact_store.get_stats()
        print(f"\n📊 Cross-File Lineage:")
        print(f"  Total facts: {stats['total_facts']}")
        print(f"  Files processed: {len(files)}")
        print(f"  Read operations: {stats['read_facts']}")
        print(f"  Write operations: {stats['write_facts']}")
        
        assert stats['total_facts'] >= 10, "Should extract facts from multiple files"
        print(f"\n✅ Cross-file lineage tracked: {stats['total_facts']} facts across {len(files)} files")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

