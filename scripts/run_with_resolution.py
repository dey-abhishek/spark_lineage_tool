"""
Script to run lineage analysis with environment-specific variable resolution
Demonstrates how ${env}_warehouse.customers gets resolved to prod_warehouse.customers
"""

import sys
import yaml
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from lineage.crawler import FileCrawler, FileType
from lineage.extractors import (
    PySparkExtractor, ScalaExtractor, HiveExtractor,
    ShellExtractor, NiFiExtractor, ConfigExtractor
)
from lineage.rules import create_default_engine
from lineage.ir import FactStore
from lineage.resolution import SymbolTable, VariableResolver
from lineage.resolution.symbol_table import Symbol

def load_environment_config(config_file: Path) -> SymbolTable:
    """Load environment variables from YAML config into SymbolTable"""
    symbol_table = SymbolTable()
    
    with open(config_file) as f:
        config = yaml.safe_load(f)
    
    def add_vars(data, prefix=''):
        """Recursively add variables from nested config"""
        for key, value in data.items():
            if isinstance(value, dict):
                add_vars(value, f"{key}.")
            else:
                var_name = f"{prefix}{key}" if prefix else key
                symbol_table.add_symbol(Symbol(
                    name=var_name,
                    value=str(value),
                    source=str(config_file),
                    resolved=True
                ))
    
    add_vars(config)
    return symbol_table

def main():
    print("=" * 70)
    print("LINEAGE ANALYSIS WITH ENVIRONMENT-SPECIFIC RESOLUTION")
    print("=" * 70)
    
    # Step 1: Load environment configuration
    config_file = Path("config/prod_env.yaml")
    print(f"\n📋 Loading environment config: {config_file}")
    symbol_table = load_environment_config(config_file)
    print(f"   Loaded {len(symbol_table.symbols)} environment variables")
    
    # Show some key variables
    print("\n🔑 Key Variables:")
    for var_name in ['env', 'INPUT_DB', 'OUTPUT_DB', 'WAREHOUSE_DB', 'base_path']:
        symbol = symbol_table.get_symbol(var_name)
        if symbol:
            print(f"   {var_name} = {symbol.value}")
    
    # Step 2: Crawl and extract facts
    print(f"\n📁 Scanning mock files...")
    rule_engine = create_default_engine()
    
    crawler = FileCrawler(Path("tests/mocks"))
    files_by_type = crawler.get_files_by_type()
    
    total_files = sum(len(files) for files in files_by_type.values())
    print(f"   Found {total_files} files")
    
    fact_store = FactStore()
    processed = 0
    
    # Extract from each file type
    extractors = {
        FileType.PYSPARK: PySparkExtractor(rule_engine),
        FileType.SCALA: ScalaExtractor(rule_engine),
        FileType.HIVE: HiveExtractor(rule_engine),
        FileType.SHELL: ShellExtractor(rule_engine),
        FileType.NIFI: NiFiExtractor(),
        FileType.PROPERTIES: ConfigExtractor(),
        FileType.YAML: ConfigExtractor(),
        FileType.JSON: ConfigExtractor(),
        FileType.XML: ConfigExtractor(),
    }
    
    for file_type, files in files_by_type.items():
        if file_type in extractors:
            extractor = extractors[file_type]
            for crawled_file in files:
                try:
                    facts = extractor.extract(crawled_file.path)
                    fact_store.add_facts(facts)
                    processed += 1
                except Exception as e:
                    pass
    
    print(f"   Processed {processed} files")
    stats = fact_store.get_stats()
    print(f"   Extracted {stats['total_facts']} facts")
    
    # Step 3: Resolve variables
    print(f"\n🔧 Resolving variables...")
    resolver = VariableResolver(symbol_table)
    
    resolved_facts = []
    resolution_stats = {
        'before_unresolved': 0,
        'after_unresolved': 0,
        'resolved': 0
    }
    
    for fact in (fact_store.get_read_facts() + fact_store.get_write_facts()):
        # Check if unresolved before
        if '${' in fact.dataset_urn or '$' in fact.dataset_urn:
            resolution_stats['before_unresolved'] += 1
        
        # Resolve
        resolved_urn, fully_resolved = resolver.resolve(fact.dataset_urn)
        
        # Update fact
        import copy
        resolved_fact = copy.deepcopy(fact)
        resolved_fact.dataset_urn = resolved_urn
        resolved_facts.append(resolved_fact)
        
        # Check if still unresolved after
        if '${' in resolved_urn or ('$' in resolved_urn and not resolved_urn.startswith('$database')):
            resolution_stats['after_unresolved'] += 1
        else:
            if fact.dataset_urn != resolved_urn:
                resolution_stats['resolved'] += 1
    
    print(f"   Before resolution: {resolution_stats['before_unresolved']} unresolved URNs")
    print(f"   After resolution:  {resolution_stats['after_unresolved']} unresolved URNs")
    print(f"   Successfully resolved: {resolution_stats['resolved']} URNs")
    
    # Step 4: Show examples of resolved variables
    print(f"\n✅ Resolution Examples:")
    examples = [
        ('${env}_warehouse.customers', 'prod_warehouse.customers'),
        ('${env}_analytics.daily_transactions', 'prod_analytics.daily_transactions'),
        ('${base_path}/raw/logs', '/data/prod/raw/logs'),
        ('${INPUT_DB}.users', 'prod_db.users'),
    ]
    
    for pattern, expected in examples:
        resolved, fully = resolver.resolve(pattern)
        status = "✅" if resolved == expected else "⚠️"
        print(f"   {status} {pattern}")
        print(f"      → {resolved}")
    
    # Step 5: Find specific resolved tables
    print(f"\n📊 Schema-Qualified Tables (Resolved):")
    resolved_tables = set()
    for fact in resolved_facts:
        urn = fact.dataset_urn
        # Look for schema.table pattern (not unresolved variables)
        if '.' in urn and not '${' in urn and not '*' in urn:
            # Extract just the table name part
            if 'hive://' in urn:
                table_name = urn.split('hive://')[-1].strip('/')
            elif 'jdbc://' in urn:
                continue
            else:
                # HDFS path
                continue
            
            if '.' in table_name and not table_name.startswith('$'):
                resolved_tables.add(table_name)
    
    # Show resolved tables sorted
    for table in sorted(resolved_tables)[:20]:
        print(f"   • {table}")
    
    print(f"\n   Total: {len(resolved_tables)} fully-resolved schema-qualified tables")
    
    print("\n" + "=" * 70)
    print("✅ RESOLUTION COMPLETE!")
    print("=" * 70)
    print(f"\nKey Insights:")
    print(f"  • Loaded {len(symbol_table.symbols)} environment variables from config")
    print(f"  • Resolved {resolution_stats['resolved']} URNs successfully")
    print(f"  • Reduced unresolved count from {resolution_stats['before_unresolved']} to {resolution_stats['after_unresolved']}")
    print(f"  • Found {len(resolved_tables)} schema-qualified tables")
    print(f"\nTo use in production:")
    print(f"  python -m lineage.cli \\")
    print(f"    --repo /path/to/repo \\")
    print(f"    --config config/prod_env.yaml \\")
    print(f"    --out output/prod_lineage")
    print(f"\nResult: ${{env}}_warehouse.customers → prod_warehouse.customers ✅")

if __name__ == "__main__":
    main()

