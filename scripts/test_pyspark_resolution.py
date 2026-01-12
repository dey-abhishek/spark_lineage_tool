"""
Test PySpark variable extraction and resolution specifically
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from lineage.extractors import PySparkExtractor
from lineage.rules import create_default_engine
from lineage.resolution import SymbolTable, VariableResolver
from lineage.resolution.symbol_table import Symbol

def test_pyspark_variable_extraction():
    """Test PySpark variable extraction with a simple example"""
    
    print("=" * 80)
    print("TEST: PySpark Variable Extraction & Resolution")
    print("=" * 80)
    
    # Create a PySpark script with variables
    pyspark_code = '''
"""PySpark ETL with variables"""

from pyspark.sql import SparkSession

# Configuration variables (should be extracted)
source_db = "prod_db"
target_db = "analytics_db"
env = "prod"
warehouse_db = "warehouse_db"
bronze_path = "/data/bronze"
silver_path = "/data/silver"

# Initialize Spark
spark = SparkSession.builder.appName("ETL").enableHiveSupport().getOrCreate()

# Read data using variables
customers_df = spark.table(f"{source_db}.customers")
orders_df = spark.table(f"{source_db}.orders")

# Write data using variables
customers_df.write.mode("overwrite").saveAsTable(f"{target_db}.customer_summary")
orders_df.write.mode("overwrite").parquet(f"{silver_path}/orders")

# Use composite variables
enriched_df = spark.sql(f"""
    SELECT * FROM {source_db}.transactions
    WHERE date >= '2024-01-01'
""")

enriched_df.write.saveAsTable(f"{env}_warehouse.enriched_transactions")
'''
    
    # Extract facts
    print("\n📋 Extracting facts from PySpark code...")
    rule_engine = create_default_engine()
    extractor = PySparkExtractor(rule_engine)
    
    facts = extractor.extract_from_content(pyspark_code, "test_pyspark.py")
    
    # Separate config facts from lineage facts
    config_facts = [f for f in facts if hasattr(f, 'config_key')]
    read_facts = [f for f in facts if hasattr(f, 'fact_type') and f.fact_type.name == 'READ']
    write_facts = [f for f in facts if hasattr(f, 'fact_type') and f.fact_type.name == 'WRITE']
    
    print(f"✅ Extracted {len(facts)} total facts")
    print(f"   - {len(config_facts)} config facts (variables)")
    print(f"   - {len(read_facts)} read facts")
    print(f"   - {len(write_facts)} write facts")
    
    # Show extracted variables
    print("\n🔑 Extracted Variables:")
    for fact in config_facts:
        print(f"   {fact.config_key} = {fact.config_value}")
    
    # Build symbol table
    print("\n📊 Building Symbol Table...")
    symbol_table = SymbolTable()
    
    for fact in config_facts:
        if hasattr(fact, 'config_key') and hasattr(fact, 'config_value'):
            symbol_table.add_symbol(Symbol(
                name=fact.config_key,
                value=fact.config_value,
                source="test_pyspark.py",
                resolved=True
            ))
    
    print(f"✅ Symbol table contains {len(symbol_table.symbols)} variables")
    
    # Show lineage facts before resolution
    print("\n📄 Lineage Facts (Before Resolution):")
    for fact in (read_facts + write_facts)[:5]:
        fact_type = "READ" if fact in read_facts else "WRITE"
        print(f"   [{fact_type}] {fact.dataset_urn}")
    
    # Resolve variables in lineage facts
    print("\n🔧 Resolving Variables...")
    resolver = VariableResolver(symbol_table)
    
    resolved_facts = []
    resolution_count = 0
    
    for fact in (read_facts + write_facts):
        if fact.dataset_urn:
            resolved_urn, fully = resolver.resolve(fact.dataset_urn)
            if resolved_urn != fact.dataset_urn:
                resolution_count += 1
                print(f"   ✅ {fact.dataset_urn}")
                print(f"      → {resolved_urn}")
            resolved_facts.append((fact, resolved_urn, fully))
    
    print(f"\n✅ Resolved {resolution_count} URNs")
    
    # Test specific resolution patterns
    print("\n🎯 Testing Specific Patterns:")
    test_patterns = [
        "hive://${source_db}.customers",
        "hive://${target_db}.customer_summary",
        "${silver_path}/orders",
        "hive://${env}_warehouse.enriched_transactions",
    ]
    
    for pattern in test_patterns:
        resolved, fully = resolver.resolve(pattern)
        status = "✅" if fully and resolved != pattern else "⚠️"
        print(f"   {status} {pattern:50s} → {resolved}")
    
    # Summary
    print("\n" + "=" * 80)
    if len(config_facts) > 0 and resolution_count > 0:
        print("✅ PYSPARK VARIABLE RESOLUTION IS WORKING!")
    else:
        print("⚠️  ISSUE DETECTED - CHECK IMPLEMENTATION")
    print("=" * 80)
    
    summary = f"""
Summary:
  • Variables Extracted: {len(config_facts)}
  • Read Operations:    {len(read_facts)}
  • Write Operations:   {len(write_facts)}
  • URNs Resolved:      {resolution_count}
  
Status: {"✅ WORKING" if len(config_facts) > 0 and resolution_count > 0 else "⚠️  NEEDS ATTENTION"}
"""
    
    print(summary)
    
    return len(config_facts) > 0 and resolution_count > 0

if __name__ == "__main__":
    success = test_pyspark_variable_extraction()
    sys.exit(0 if success else 1)

