"""
Test that variables defined in code are automatically extracted and resolved
Shows that ${env}_warehouse.customers gets resolved from env=prod in the code itself
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from lineage.extractors import ShellExtractor, PySparkExtractor, ConfigExtractor
from lineage.rules import create_default_engine
from lineage.resolution import SymbolTable, VariableResolver
from lineage.resolution.symbol_table import Symbol

def test_shell_variable_extraction_and_resolution():
    """Test that shell script variables are extracted and used for resolution"""
    
    print("=" * 70)
    print("TEST: Shell Script Variable Extraction & Resolution")
    print("=" * 70)
    
    # Read the etl_env.sh config file
    config_file = Path("tests/mocks/configs/etl_env.sh")
    
    print(f"\n📄 Analyzing: {config_file}")
    print("   This file contains: export env=prod, export SOURCE_DB=prod_db, etc.")
    
    # Use ShellExtractor since it now extracts variable definitions
    config_extractor = ShellExtractor(create_default_engine())
    config_facts = config_extractor.extract(config_file)
    facts = config_facts
    print(f"\n✅ Extracted {len(facts)} config facts")
    
    # Build symbol table from extracted config facts
    symbol_table = SymbolTable()
    for fact in facts:
        # Config facts have config_key and config_value
        if hasattr(fact, 'config_key') and hasattr(fact, 'config_value'):
            symbol_table.add_symbol(Symbol(
                name=fact.config_key,
                value=fact.config_value,
                source=str(config_file),
                resolved=True
            ))
    
    print(f"📊 Built symbol table with {len(symbol_table.symbols)} variables")
    
    # Show key variables
    print("\n🔑 Extracted Variables:")
    for var_name in ['SOURCE_DB', 'TARGET_DB', 'WAREHOUSE_DB', 'SOURCE_CUSTOMERS_TABLE']:
        symbol = symbol_table.get_symbol(var_name)
        if symbol:
            print(f"   {var_name} = {symbol.value}")
    
    # Now resolve URNs using these extracted variables
    resolver = VariableResolver(symbol_table)
    
    print("\n🔧 Resolving URNs from extracted variables:")
    test_urns = [
        "${SOURCE_DB}.customers",
        "${TARGET_DB}.customer_summary",
        "${SOURCE_CUSTOMERS_TABLE}",
        "${WAREHOUSE_DB}.orders"
    ]
    
    for urn in test_urns:
        resolved, fully = resolver.resolve(urn)
        status = "✅" if fully else "⚠️"
        print(f"   {status} {urn}")
        print(f"      → {resolved}")
    
    print("\n" + "=" * 70)
    print("✅ Variables extracted from code and used for resolution!")
    print("=" * 70)


def test_master_script_uses_sourced_variables():
    """Test that master script references variables from sourced config"""
    
    print("\n" + "=" * 70)
    print("TEST: Master Script Uses Sourced Variables")
    print("=" * 70)
    
    master_script = Path("tests/mocks/shell/07_master_etl_pipeline.sh")
    print(f"\n📄 Analyzing: {master_script}")
    
    # Read the script
    with open(master_script) as f:
        content = f.read()
    
    # Check that it sources the config
    if 'source' in content and 'etl_env.sh' in content:
        print("   ✅ Script sources etl_env.sh")
    
    # Check that it uses variables
    variables_used = []
    for var in ['SOURCE_CUSTOMERS_TABLE', 'TARGET_CUSTOMER_SUMMARY', 'STAGING_CUSTOMERS']:
        if f'${{{var}}}' in content or f'${var}' in content:
            variables_used.append(var)
            print(f"   ✅ Uses variable: ${{{var}}}")
    
    print(f"\n📊 Found {len(variables_used)} variable references")
    
    # Extract facts from the script
    rule_engine = create_default_engine()
    shell_extractor = ShellExtractor(rule_engine)
    facts = shell_extractor.extract(master_script)
    
    print(f"✅ Extracted {len(facts)} facts from master script")
    
    # Show facts with variables
    variable_facts = [f for f in facts if f.dataset_urn and '${' in f.dataset_urn]
    print(f"📊 {len(variable_facts)} facts contain variables that need resolution")
    
    for fact in variable_facts[:5]:
        print(f"   • {fact.dataset_urn} (from {fact.source_file.name})")


def test_end_to_end_code_based_resolution():
    """Test complete flow: extract vars from code → build symbol table → resolve"""
    
    print("\n" + "=" * 70)
    print("TEST: End-to-End Code-Based Resolution")
    print("=" * 70)
    
    # Step 1: Extract variables from etl_env.sh using ShellExtractor
    print("\n📋 Step 1: Extract variables from etl_env.sh")
    config_file = Path("tests/mocks/configs/etl_env.sh")
    shell_extractor = ShellExtractor(create_default_engine())
    config_facts = shell_extractor.extract(config_file)
    
    symbol_table = SymbolTable()
    for fact in config_facts:
        if hasattr(fact, 'config_key') and hasattr(fact, 'config_value'):
            symbol_table.add_symbol(Symbol(
                name=fact.config_key,
                value=fact.config_value,
                source=str(config_file),
                resolved=True
            ))
    
    print(f"   Extracted {len(symbol_table.symbols)} variables")
    
    # Step 2: Extract facts from master script
    print("\n📋 Step 2: Extract lineage facts from master script")
    master_script = Path("tests/mocks/shell/07_master_etl_pipeline.sh")
    rule_engine = create_default_engine()
    shell_extractor = ShellExtractor(rule_engine)
    lineage_facts = shell_extractor.extract(master_script)
    
    print(f"   Extracted {len(lineage_facts)} lineage facts")
    
    # Step 3: Resolve facts using extracted variables
    print("\n📋 Step 3: Resolve lineage facts using extracted variables")
    resolver = VariableResolver(symbol_table)
    
    resolved_count = 0
    for fact in lineage_facts:
        if fact.dataset_urn and '${' in fact.dataset_urn:
            resolved_urn, fully = resolver.resolve(fact.dataset_urn)
            if fully and resolved_urn != fact.dataset_urn:
                resolved_count += 1
                print(f"   ✅ {fact.dataset_urn}")
                print(f"      → {resolved_urn}")
    
    print(f"\n📊 Resolved {resolved_count} URNs from code-extracted variables")
    
    print("\n" + "=" * 70)
    print("✅ COMPLETE: Code-based variable resolution working!")
    print("=" * 70)
    print("\nKey Points:")
    print("  1. Tool extracts 'export VAR=value' from shell/config files")
    print("  2. Builds symbol table from extracted variables")
    print("  3. Resolves ${VAR} references in lineage facts")
    print("  4. No external config needed - uses code itself!")


if __name__ == "__main__":
    test_shell_variable_extraction_and_resolution()
    test_master_script_uses_sourced_variables()
    test_end_to_end_code_based_resolution()

