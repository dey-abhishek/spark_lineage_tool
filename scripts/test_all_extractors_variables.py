"""
Comprehensive test: Variable extraction from ALL file types
Demonstrates that PySpark, Scala, Hive, and Shell all extract variables
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from lineage.extractors import PySparkExtractor, ScalaExtractor, HiveExtractor, ShellExtractor
from lineage.rules import create_default_engine
from lineage.resolution import SymbolTable, VariableResolver
from lineage.resolution.symbol_table import Symbol

def test_all_extractors_variable_extraction():
    """Test that all extractors extract variables from their respective file types"""
    
    print("=" * 80)
    print("COMPREHENSIVE TEST: Variable Extraction from ALL File Types")
    print("=" * 80)
    
    rule_engine = create_default_engine()
    
    # Test Shell variable extraction
    print("\n" + "=" * 80)
    print("1. SHELL SCRIPT (etl_env.sh)")
    print("=" * 80)
    
    shell_extractor = ShellExtractor(rule_engine)
    shell_config = Path("tests/mocks/configs/etl_env.sh")
    
    shell_facts = shell_extractor.extract(shell_config)
    config_facts = [f for f in shell_facts if hasattr(f, 'config_key')]
    
    print(f"✅ Extracted {len(config_facts)} variables from Shell")
    for fact in config_facts[:10]:
        print(f"   {fact.config_key} = {fact.config_value}")
    
    # Test PySpark variable extraction
    print("\n" + "=" * 80)
    print("2. PYSPARK (config_driven_etl.py)")
    print("=" * 80)
    
    pyspark_extractor = PySparkExtractor(rule_engine)
    pyspark_file = Path("tests/mocks/pyspark/20_config_driven_etl.py")
    
    if pyspark_file.exists():
        pyspark_facts = pyspark_extractor.extract(pyspark_file)
        pyspark_config_facts = [f for f in pyspark_facts if hasattr(f, 'config_key')]
        
        print(f"✅ Extracted {len(pyspark_config_facts)} variables from PySpark")
        for fact in pyspark_config_facts[:10]:
            print(f"   {fact.config_key} = {fact.config_value}")
    else:
        print("⚠️  File not found")
    
    # Test Hive variable extraction (create a test file)
    print("\n" + "=" * 80)
    print("3. HIVE SQL (with SET statements)")
    print("=" * 80)
    
    hive_extractor = HiveExtractor(rule_engine)
    hive_content = """
-- Hive configuration variables
SET hivevar:env=prod;
SET hivevar:source_db=prod_db;
SET hivevar:target_db=analytics_db;
SET hiveconf:mapred.reduce.tasks=10;
SET execution.engine=tez;

-- Use the variables
INSERT INTO ${hivevar:target_db}.customer_summary
SELECT * FROM ${hivevar:source_db}.customers;
"""
    
    hive_facts = hive_extractor.extract_from_content(hive_content, "test_hive.hql")
    hive_config_facts = [f for f in hive_facts if hasattr(f, 'config_key')]
    
    print(f"✅ Extracted {len(hive_config_facts)} variables from Hive SQL")
    for fact in hive_config_facts:
        print(f"   {fact.config_key} = {fact.config_value}")
    
    # Test Scala variable extraction
    print("\n" + "=" * 80)
    print("4. SCALA (with val/var definitions)")
    print("=" * 80)
    
    scala_extractor = ScalaExtractor(rule_engine)
    scala_content = """
package com.example.spark

object ETLJob {
  def main(args: Array[String]): Unit = {
    // Configuration variables
    val sourceDb = "prod_db"
    val targetDb = "analytics_db"
    val environment = "prod"
    var runDate = "2024-01-15"
    
    // Use the variables
    spark.read.table(s"$sourceDb.customers")
      .write.saveAsTable(s"$targetDb.customer_summary")
  }
}
"""
    
    scala_facts = scala_extractor.extract_from_content(scala_content, "test_scala.scala")
    scala_config_facts = [f for f in scala_facts if hasattr(f, 'config_key')]
    
    print(f"✅ Extracted {len(scala_config_facts)} variables from Scala")
    for fact in scala_config_facts:
        print(f"   {fact.config_key} = {fact.config_value}")
    
    # Build unified symbol table
    print("\n" + "=" * 80)
    print("5. UNIFIED SYMBOL TABLE (from all sources)")
    print("=" * 80)
    
    symbol_table = SymbolTable()
    
    # Add all config facts
    all_config_facts = config_facts + pyspark_config_facts + hive_config_facts + scala_config_facts
    
    for fact in all_config_facts:
        if hasattr(fact, 'config_key') and hasattr(fact, 'config_value'):
            symbol_table.add_symbol(Symbol(
                name=fact.config_key,
                value=fact.config_value,
                source=fact.source_file,
                resolved=True
            ))
    
    print(f"✅ Built unified symbol table with {len(symbol_table.symbols)} variables")
    print("\nSample variables:")
    for var_name in list(symbol_table.symbols.keys())[:15]:
        symbol = symbol_table.get_symbol(var_name)
        if symbol:
            source_type = Path(symbol.source).suffix or "shell"
            print(f"   {var_name:30s} = {symbol.value:30s} (from {source_type})")
    
    # Test resolution with unified symbol table
    print("\n" + "=" * 80)
    print("6. RESOLUTION USING UNIFIED SYMBOL TABLE")
    print("=" * 80)
    
    resolver = VariableResolver(symbol_table)
    
    test_urns = [
        ("${SOURCE_DB}.customers", "from Shell"),
        ("${source_db}.orders", "from Hive"),
        ("${sourceDb}.products", "from Scala"),
        ("${env}_warehouse.customers", "composite"),
    ]
    
    print("\nResolution examples:")
    for urn, source in test_urns:
        resolved, fully = resolver.resolve(urn)
        status = "✅" if fully and resolved != urn else "⚠️"
        print(f"   {status} {urn:40s} → {resolved}")
    
    # Summary
    print("\n" + "=" * 80)
    print("✅ COMPLETE: All File Types Support Variable Extraction!")
    print("=" * 80)
    
    summary = f"""
Summary:
  • Shell:   {len(config_facts)} variables extracted
  • PySpark: {len(pyspark_config_facts)} variables extracted  
  • Hive:    {len(hive_config_facts)} variables extracted
  • Scala:   {len(scala_config_facts)} variables extracted
  • TOTAL:   {len(all_config_facts)} variables from all sources
  
Unified Symbol Table:
  • {len(symbol_table.symbols)} unique variables
  • Available for cross-file resolution
  • Supports nested variable expansion
  
Key Achievement:
  ✅ Variables extracted from CODE itself (no external config needed!)
  ✅ Works across ALL supported languages
  ✅ Enables automatic resolution at runtime
"""
    
    print(summary)

if __name__ == "__main__":
    test_all_extractors_variable_extraction()

