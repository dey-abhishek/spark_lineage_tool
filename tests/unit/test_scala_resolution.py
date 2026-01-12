"""
Unit test for Scala conditional default extraction.
"""

import pytest
from lineage.extractors.scala_extractor import ScalaExtractor
from lineage.resolution.resolver import VariableResolver
from lineage.resolution.symbol_table import SymbolTable, Symbol


class TestScalaConditionalDefaults:
    """Test Scala conditional assignment with defaults."""
    
    def test_extract_conditional_defaults(self):
        """Test extraction of val x = if (...) ... else 'default' pattern."""
        code = '''
object TestJob {
  def main(args: Array[String]): Unit = {
    val runDate = if (args.length > 0) args(0) else "2024-01-01"
    val env = if (args.length > 1) args(1) else "prod"
    
    // Use the variables
    val path = s"/data/$env/transactions/$runDate"
  }
}
'''
        
        extractor = ScalaExtractor()
        facts = extractor.extract_from_content(code, "test.scala")
        
        # Find ConfigFacts
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        # Should extract default values
        assert "runDate" in config_dict, "runDate not extracted"
        assert config_dict["runDate"] == "2024-01-01", \
            f"Expected '2024-01-01', got '{config_dict['runDate']}'"
        
        assert "env" in config_dict, "env not extracted"
        assert config_dict["env"] == "prod", \
            f"Expected 'prod', got '{config_dict['env']}'"
    
    def test_resolution_with_conditional_defaults(self):
        """Test that conditional defaults are properly resolved in paths."""
        code = '''
object MultiFormatJob {
  def main(args: Array[String]): Unit = {
    val runDate = if (args.length > 0) args(0) else "2024-01-01"
    val env = if (args.length > 1) args(1) else "prod"
    val basePath = s"/data/$env"
    
    // Paths using the variables
    spark.read.parquet(s"$basePath/raw/transactions/$runDate/*.parquet")
  }
}
'''
        
        extractor = ScalaExtractor()
        facts = extractor.extract_from_content(code, "test.scala")
        
        # Build symbol table
        symbol_table = SymbolTable()
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        for fact in config_facts:
            symbol_table.add_symbol(Symbol(fact.config_key, fact.config_value, "scala"))
        
        # Try to resolve paths
        resolver = VariableResolver(symbol_table)
        
        # Find read facts
        read_facts = [f for f in facts if f.__class__.__name__ == 'ReadFact']
        
        for fact in read_facts:
            if '$' in fact.dataset_urn:
                resolved_urn, _ = resolver.resolve(fact.dataset_urn)
                
                # Should have runDate and env resolved
                assert "$runDate" not in resolved_urn, \
                    f"$runDate not resolved in: {resolved_urn}"
                assert "$env" not in resolved_urn, \
                    f"$env not resolved in: {resolved_urn}"
                
                # Should contain the default values
                assert "2024-01-01" in resolved_urn or "prod" in resolved_urn, \
                    f"Default values not in resolved path: {resolved_urn}"
    
    def test_nested_conditionals(self):
        """Test nested conditional assignments."""
        code = '''
val config = if (mode == "prod") "prod_config" else "dev_config"
val db = if (config == "prod_config") "prod_db" else "dev_db"
'''
        
        extractor = ScalaExtractor()
        facts = extractor.extract_from_content(code, "test.scala")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        config_dict = {f.config_key: f.config_value for f in config_facts}
        
        # Should extract both defaults
        assert "config" in config_dict
        assert "db" in config_dict
        assert config_dict["config"] == "dev_config"
        assert config_dict["db"] == "dev_db"
    
    def test_conditional_confidence_level(self):
        """Test that conditional defaults have appropriate confidence."""
        code = 'val runDate = if (args.length > 0) args(0) else "2024-01-01"'
        
        extractor = ScalaExtractor()
        facts = extractor.extract_from_content(code, "test.scala")
        
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        
        assert len(config_facts) > 0, "No ConfigFacts extracted"
        
        for fact in config_facts:
            if fact.config_key == "runDate":
                # Conditional defaults should have slightly lower confidence than direct assignments
                assert 0.80 <= fact.confidence <= 0.90, \
                    f"Unexpected confidence: {fact.confidence}"
                assert fact.config_source == "scala_conditional_default"


class TestScalaRegressionPrevention:
    """Prevent regression of Scala resolution issues."""
    
    def test_no_unresolved_rundate_in_paths(self):
        """Ensure $runDate never appears literally in extracted paths."""
        code = '''
val runDate = if (args.length > 0) args(0) else "2024-01-01"
spark.read.parquet(s"/data/raw/$runDate/*.parquet")
spark.write.parquet(s"/data/processed/$runDate")
'''
        
        extractor = ScalaExtractor()
        facts = extractor.extract_from_content(code, "test.scala")
        
        # Build symbol table and resolve
        symbol_table = SymbolTable()
        config_facts = [f for f in facts if f.__class__.__name__ == 'ConfigFact']
        for fact in config_facts:
            symbol_table.add_symbol(Symbol(fact.config_key, fact.config_value, "scala"))
        
        resolver = VariableResolver(symbol_table)
        
        # Check all facts
        for fact in facts:
            if hasattr(fact, 'dataset_urn') and fact.dataset_urn:
                resolved_urn, _ = resolver.resolve(fact.dataset_urn)
                
                # Should NOT have $runDate in resolved path
                assert "$runDate" not in resolved_urn, \
                    f"Found unresolved $runDate in: {resolved_urn}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

