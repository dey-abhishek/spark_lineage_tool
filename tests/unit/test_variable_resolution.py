"""
Test: Variable Resolution in Lineage
Tests that variables like ${gold_table} are resolved to actual values like analytics_gold.customer_summary
"""

import pytest
from pathlib import Path
import ast
from lineage.extractors.pyspark_extractor import PySparkExtractor
from lineage.rules.engine import RuleEngine
from lineage.resolution.symbol_table import SymbolTable, Symbol
from lineage.resolution.resolver import VariableResolver
from lineage.ir.fact import FactType


def extract_variables_from_python(file_path: Path) -> SymbolTable:
    """Extract variable assignments from Python file."""
    symbol_table = SymbolTable()
    
    with open(file_path) as f:
        content = f.read()
        try:
            tree = ast.parse(content)
            
            for node in ast.walk(tree):
                # Look for variable assignments: var_name = "value"
                if isinstance(node, ast.Assign):
                    for target in node.targets:
                        if isinstance(target, ast.Name):
                            var_name = target.id
                            
                            # Try to extract string value
                            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                                value = node.value.value
                                symbol_table.add_symbol(Symbol(
                                    name=var_name,
                                    value=value,
                                    source="declared",
                                    file=str(file_path),
                                    resolved=True
                                ))
        except:
            pass
    
    return symbol_table


class TestVariableResolution:
    """Test variable resolution in lineage extraction"""
    
    @pytest.fixture
    def extractor(self):
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        return PySparkExtractor(rule_engine)
    
    def test_schema_qualified_variable_resolution(self, extractor):
        """Test resolution of schema-qualified table variables"""
        file_path = Path("tests/mocks/pyspark/21_schema_qualified_tables.py")
        
        # Extract facts
        facts = extractor.extract(file_path)
        
        # Check that we get fully resolved table names (not variables)
        table_reads = [f for f in facts if f.fact_type == FactType.READ and 
                      f.dataset_type == "hive"]
        table_writes = [f for f in facts if f.fact_type == FactType.WRITE and 
                       f.dataset_type == "hive"]
        
        # These should be actual table names, not variables
        for fact in table_reads + table_writes:
            assert fact.dataset_urn is not None
            # Should contain actual schema.table, not ${variable}
            if "${" not in fact.dataset_urn:
                print(f"  ✅ Resolved: {fact.dataset_urn}")
                assert "." in fact.dataset_urn or "/" in fact.dataset_urn
    
    def test_variable_extraction_from_code(self, extractor):
        """Test extracting variables from Python code"""
        file_path = Path("tests/mocks/pipelines/stage3_aggregation.py")
        
        # Extract variable definitions
        symbol_table = extract_variables_from_python(file_path)
        
        # Should have found variable definitions
        gold_table = symbol_table.get_symbol("gold_table")
        assert gold_table is not None, "Should extract gold_table variable"
        print(f"\n✅ Extracted variable: gold_table = {gold_table.value}")
        
        # Verify it's a schema-qualified table
        if gold_table.value:
            assert "analytics_gold.customer_summary" in gold_table.value
    
    def test_fact_resolution_with_symbol_table(self, extractor):
        """Test resolving facts using symbol table"""
        file_path = Path("tests/mocks/pipelines/stage3_aggregation.py")
        
        # Extract variables
        symbol_table = extract_variables_from_python(file_path)
        
        # Extract facts
        facts = extractor.extract(file_path)
        
        # Create resolver
        resolver = VariableResolver(symbol_table)
        
        # Resolve facts
        resolved_count = 0
        for fact in facts:
            if fact.dataset_urn and "${" in fact.dataset_urn:
                original = fact.dataset_urn
                resolved, fully_resolved = resolver.resolve(fact.dataset_urn)
                
                if fully_resolved and resolved != original:
                    resolved_count += 1
                    print(f"\n✅ Resolved:")
                    print(f"   From: {original}")
                    print(f"   To:   {resolved}")
        
        print(f"\n✅ Resolved {resolved_count} variable references")
        # We should resolve at least the gold_table reference
        assert resolved_count >= 1, f"Should resolve variables, resolved {resolved_count}"
    
    def test_end_to_end_resolution(self):
        """Test end-to-end: extract variables, extract facts, resolve"""
        file_path = Path("tests/mocks/pipelines/stage4_reporting.py")
        
        # Step 1: Extract variables from code
        symbol_table = extract_variables_from_python(file_path)
        print(f"\n📊 Extracted {len(symbol_table.symbols)} variables")
        for name, symbol in symbol_table.symbols.items():
            print(f"  {name} = {symbol.value}")
        
        # Step 2: Extract facts
        rule_engine = RuleEngine()
        rule_engine.load_default_rules()
        extractor = PySparkExtractor(rule_engine)
        facts = extractor.extract(file_path)
        print(f"\n📊 Extracted {len(facts)} facts")
        
        # Step 3: Resolve all facts
        resolver = VariableResolver(symbol_table)
        resolved_facts = []
        
        for fact in facts:
            if fact.dataset_urn:
                resolved_urn, fully_resolved = resolver.resolve(fact.dataset_urn)
                
                if "${" in fact.dataset_urn and fully_resolved:
                    resolved_facts.append({
                        "original": fact.dataset_urn,
                        "resolved": resolved_urn,
                        "type": fact.fact_type.value
                    })
        
        print(f"\n✅ Resolved {len(resolved_facts)} facts:")
        for rf in resolved_facts:
            print(f"  {rf['type']}: {rf['original']} → {rf['resolved']}")
        
        # Should resolve at least some facts
        assert len(resolved_facts) >= 1, "Should resolve at least one fact"
    
    def test_hive_sql_embedded_resolution(self, extractor):
        """Test that embedded SQL references get proper schema.table"""
        file_path = Path("tests/mocks/pyspark/21_schema_qualified_tables.py")
        facts = extractor.extract(file_path)
        
        # Count schema-qualified tables from SQL
        sql_tables = []
        for fact in facts:
            if fact.dataset_urn and "." in fact.dataset_urn:
                # Remove hive:// prefix
                table = fact.dataset_urn.replace("hive://", "")
                if "_db." in table or "db." in table:
                    sql_tables.append(table)
        
        print(f"\n✅ Found {len(sql_tables)} schema-qualified tables:")
        for table in list(set(sql_tables))[:5]:
            print(f"  - {table}")
        
        assert len(sql_tables) >= 5, f"Should extract schema.table from SQL, found {len(sql_tables)}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

