"""PySpark AST-based extractor."""

import ast
from pathlib import Path
from typing import List, Optional, Any, Set
import re

from .base import BaseExtractor
from lineage.ir import Fact, ReadFact, WriteFact, FactType, ExtractionMethod
from lineage.rules import RuleEngine


class PySparkASTVisitor(ast.NodeVisitor):
    """AST visitor for extracting Spark operations."""
    
    def __init__(self, source_file: str) -> None:
        self.source_file = source_file
        self.facts: List[Fact] = []
        self.variables: dict[str, Any] = {}  # Track variable assignments
    
    def visit_Call(self, node: ast.Call) -> None:
        """Visit function call nodes."""
        # Extract method chain
        call_chain = self._extract_call_chain(node)
        
        # Detect read operations
        if self._is_read_operation(call_chain):
            fact = self._extract_read_fact(node, call_chain)
            if fact:
                self.facts.append(fact)
        
        # Detect write operations
        elif self._is_write_operation(call_chain):
            fact = self._extract_write_fact(node, call_chain)
            if fact:
                self.facts.append(fact)
        
        # Detect table operations
        elif self._is_table_operation(call_chain):
            fact = self._extract_table_fact(node, call_chain)
            if fact:
                self.facts.append(fact)
        
        # Detect SQL operations
        elif self._is_sql_operation(call_chain):
            fact = self._extract_sql_fact(node, call_chain)
            if fact:
                self.facts.append(fact)
        
        self.generic_visit(node)
    
    def visit_Assign(self, node: ast.Assign) -> None:
        """Visit assignment nodes to track variables."""
        # Simple variable tracking
        for target in node.targets:
            if isinstance(target, ast.Name):
                # Store basic info about the assignment
                self.variables[target.id] = node.value
        
        self.generic_visit(node)
    
    def _extract_call_chain(self, node: ast.Call) -> List[str]:
        """Extract method/attribute call chain."""
        chain = []
        current = node.func
        
        while current:
            if isinstance(current, ast.Attribute):
                chain.insert(0, current.attr)
                current = current.value
            elif isinstance(current, ast.Name):
                chain.insert(0, current.id)
                break
            elif isinstance(current, ast.Call):
                if isinstance(current.func, ast.Attribute):
                    chain.insert(0, current.func.attr)
                    current = current.func.value
                else:
                    break
            else:
                break
        
        return chain
    
    def _is_read_operation(self, chain: List[str]) -> bool:
        """Check if call chain is a read operation."""
        read_methods = ["parquet", "csv", "json", "orc", "text", "avro", "load"]
        return "read" in chain and any(method in chain for method in read_methods)
    
    def _is_write_operation(self, chain: List[str]) -> bool:
        """Check if call chain is a write operation."""
        write_methods = ["parquet", "csv", "json", "orc", "text", "avro", "save"]
        return "write" in chain and any(method in chain for method in write_methods)
    
    def _is_table_operation(self, chain: List[str]) -> bool:
        """Check if call chain is a table operation."""
        table_methods = ["table", "saveAsTable", "insertInto"]
        return any(method in chain for method in table_methods)
    
    def _is_sql_operation(self, chain: List[str]) -> bool:
        """Check if call chain is a SQL operation."""
        return "sql" in chain
    
    def _extract_string_arg(self, node: ast.Call, pos: int = 0) -> Optional[str]:
        """Extract string argument from call."""
        if len(node.args) > pos:
            arg = node.args[pos]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                return arg.value
            elif isinstance(arg, ast.Str):  # Python 3.7 compatibility
                return arg.s
            elif isinstance(arg, ast.Name):
                # Try to resolve variable
                return f"${{{arg.id}}}"
            elif isinstance(arg, ast.JoinedStr):  # f-string
                return self._extract_fstring(arg)
        return None
    
    def _extract_fstring(self, node: ast.JoinedStr) -> str:
        """Extract f-string with placeholders."""
        parts = []
        for value in node.values:
            if isinstance(value, ast.Constant):
                parts.append(str(value.value))
            elif isinstance(value, ast.Str):
                parts.append(value.s)
            elif isinstance(value, ast.FormattedValue):
                if isinstance(value.value, ast.Name):
                    parts.append(f"${{{value.value.id}}}")
                else:
                    parts.append("${...}")
        return "".join(parts)
    
    def _extract_read_fact(self, node: ast.Call, chain: List[str]) -> Optional[Fact]:
        """Extract read fact from AST node."""
        path = self._extract_string_arg(node)
        if not path:
            return None
        
        fact = ReadFact(
            source_file=self.source_file,
            line_number=node.lineno,
            dataset_urn=path,
            dataset_type="hdfs",
            confidence=0.85,
            extraction_method=ExtractionMethod.AST,
            evidence=f"spark.read.{chain[-1]}({path})",
            has_placeholders="${" in path
        )
        
        # Determine format from chain
        if "parquet" in chain:
            fact.params["format"] = "parquet"
        elif "csv" in chain:
            fact.params["format"] = "csv"
        elif "json" in chain:
            fact.params["format"] = "json"
        elif "orc" in chain:
            fact.params["format"] = "orc"
        
        return fact
    
    def _extract_write_fact(self, node: ast.Call, chain: List[str]) -> Optional[Fact]:
        """Extract write fact from AST node."""
        path = self._extract_string_arg(node)
        if not path:
            return None
        
        fact = WriteFact(
            source_file=self.source_file,
            line_number=node.lineno,
            dataset_urn=path,
            dataset_type="hdfs",
            confidence=0.85,
            extraction_method=ExtractionMethod.AST,
            evidence=f"df.write.{chain[-1]}({path})",
            has_placeholders="${" in path
        )
        
        # Determine format
        if "parquet" in chain:
            fact.params["format"] = "parquet"
        elif "csv" in chain:
            fact.params["format"] = "csv"
        elif "json" in chain:
            fact.params["format"] = "json"
        elif "orc" in chain:
            fact.params["format"] = "orc"
        
        # Check for mode
        for keyword in node.keywords:
            if keyword.arg == "mode":
                if isinstance(keyword.value, ast.Constant):
                    fact.write_mode = keyword.value.value
        
        return fact
    
    def _extract_table_fact(self, node: ast.Call, chain: List[str]) -> Optional[Fact]:
        """Extract table operation fact."""
        table_name = self._extract_string_arg(node)
        if not table_name:
            return None
        
        # Determine if read or write
        if "table" in chain and "saveAsTable" not in chain and "insertInto" not in chain:
            # Read operation
            fact = ReadFact(
                source_file=self.source_file,
                line_number=node.lineno,
                dataset_urn=f"hive://{table_name}",
                dataset_type="hive",
                confidence=0.90,
                extraction_method=ExtractionMethod.AST,
                evidence=f"spark.table('{table_name}')",
                has_placeholders="${" in table_name
            )
        else:
            # Write operation (saveAsTable or insertInto)
            method = "saveAsTable" if "saveAsTable" in chain else "insertInto"
            fact = WriteFact(
                source_file=self.source_file,
                line_number=node.lineno,
                dataset_urn=f"hive://{table_name}",
                dataset_type="hive",
                confidence=0.90,
                extraction_method=ExtractionMethod.AST,
                evidence=f"df.{method}('{table_name}')",
                has_placeholders="${" in table_name
            )
        
        fact.params["table_name"] = table_name
        return fact
    
    def _extract_sql_fact(self, node: ast.Call, chain: List[str]) -> Optional[Fact]:
        """Extract SQL fact and parse embedded SQL."""
        sql = self._extract_string_arg(node)
        if not sql:
            return None
        
        # Parse SQL to extract table references
        # For now, create a fact with the SQL string
        # The SQL extractor will handle detailed parsing
        fact = Fact(
            fact_type=FactType.READ,  # Will be refined by SQL parsing
            source_file=self.source_file,
            line_number=node.lineno,
            confidence=0.75,
            extraction_method=ExtractionMethod.AST,
            evidence=f"spark.sql('{sql[:100]}...')",
            has_placeholders=True
        )
        
        fact.params["sql"] = sql
        fact.params["embedded"] = True
        
        return fact


class PySparkExtractor(BaseExtractor):
    """Extractor for PySpark files using AST parsing."""
    
    def __init__(self, rule_engine: Optional[RuleEngine] = None) -> None:
        super().__init__()
        self.rule_engine = rule_engine
    
    def extract(self, file_path: Path) -> List[Fact]:
        """Extract facts from PySpark file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            return self.extract_from_content(content, str(file_path))
        
        except Exception as e:
            print(f"Error extracting from {file_path}: {e}")
            return []
    
    def extract_from_content(self, content: str, source_file: str) -> List[Fact]:
        """Extract facts from PySpark content."""
        facts = []
        
        # Try AST-based extraction first
        try:
            tree = ast.parse(content)
            visitor = PySparkASTVisitor(source_file)
            visitor.visit(tree)
            facts.extend(visitor.facts)
        except SyntaxError as e:
            print(f"Syntax error in {source_file}: {e}")
        
        # Fallback to regex-based extraction if rule engine available
        if self.rule_engine:
            matches = self.rule_engine.apply_rules(content, "pyspark")
            for match in matches:
                fact = self._match_to_fact(match, source_file)
                if fact:
                    facts.append(fact)
        
        return facts
    
    def _match_to_fact(self, match: dict, source_file: str) -> Optional[Fact]:
        """Convert rule match to fact."""
        from lineage.rules import RuleAction
        
        action = match["action"]
        
        if action == RuleAction.READ_HDFS_PATH:
            path = match["groups"].get("path", "")
            return ReadFact(
                source_file=source_file,
                dataset_urn=path,
                dataset_type="hdfs",
                confidence=match["confidence"],
                extraction_method=ExtractionMethod.REGEX,
                evidence=match["match_text"],
                has_placeholders="${" in path or "$" in path
            )
        
        elif action == RuleAction.WRITE_HDFS_PATH:
            path = match["groups"].get("path", "")
            return WriteFact(
                source_file=source_file,
                dataset_urn=path,
                dataset_type="hdfs",
                confidence=match["confidence"],
                extraction_method=ExtractionMethod.REGEX,
                evidence=match["match_text"],
                has_placeholders="${" in path or "$" in path
            )
        
        elif action == RuleAction.READ_HIVE_TABLE:
            table = match["groups"].get("table", "")
            return ReadFact(
                source_file=source_file,
                dataset_urn=f"hive://{table}",
                dataset_type="hive",
                confidence=match["confidence"],
                extraction_method=ExtractionMethod.REGEX,
                evidence=match["match_text"],
                has_placeholders="${" in table
            )
        
        elif action == RuleAction.WRITE_HIVE_TABLE:
            table = match["groups"].get("table", "")
            return WriteFact(
                source_file=source_file,
                dataset_urn=f"hive://{table}",
                dataset_type="hive",
                confidence=match["confidence"],
                extraction_method=ExtractionMethod.REGEX,
                evidence=match["match_text"],
                has_placeholders="${" in table
            )
        
        return None
    
    def get_confidence_base(self) -> float:
        """Get base confidence for AST extraction."""
        return 0.85

