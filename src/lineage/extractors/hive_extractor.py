"""Hive SQL parser and extractor."""

from pathlib import Path
from typing import List, Optional, Set
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Where, Token
from sqlparse.tokens import Keyword, DML

from .base import BaseExtractor
from lineage.ir import Fact, ReadFact, WriteFact, ExtractionMethod
from lineage.rules import RuleEngine


class HiveSQLParser:
    """Parser for Hive SQL statements."""
    
    def __init__(self) -> None:
        self.read_tables: Set[str] = set()
        self.write_tables: Set[str] = set()
        self.paths: Set[str] = set()
    
    def parse_statement(self, sql: str) -> dict:
        """Parse a single SQL statement."""
        self.read_tables.clear()
        self.write_tables.clear()
        self.paths.clear()
        
        # Parse with sqlparse
        statements = sqlparse.parse(sql)
        
        for statement in statements:
            stmt_type = statement.get_type()
            
            if stmt_type == "INSERT":
                self._parse_insert(statement)
            elif stmt_type == "SELECT":
                self._parse_select(statement)
            elif stmt_type == "CREATE":
                self._parse_create(statement)
            elif stmt_type in ["DELETE", "UPDATE"]:
                self._parse_dml(statement)
            else:
                # Fallback to regex for complex statements
                self._parse_with_regex(sql)
        
        return {
            "read_tables": list(self.read_tables),
            "write_tables": list(self.write_tables),
            "paths": list(self.paths)
        }
    
    def _parse_insert(self, statement: sqlparse.sql.Statement) -> None:
        """Parse INSERT statement."""
        tokens = list(statement.flatten())
        
        in_insert = False
        in_from = False
        in_join = False
        
        for i, token in enumerate(tokens):
            token_text = token.value.upper()
            
            # Find target table
            if token.ttype is Keyword and token_text in ("INTO", "OVERWRITE"):
                in_insert = True
            elif in_insert and token.ttype is Keyword and token_text == "TABLE":
                # Next non-keyword token is the table name
                for j in range(i + 1, len(tokens)):
                    if tokens[j].ttype not in (Keyword, sqlparse.tokens.Whitespace):
                        table_name = self._clean_identifier(tokens[j].value)
                        self.write_tables.add(table_name)
                        in_insert = False
                        break
            
            # Find source tables
            if token.ttype is Keyword and token_text == "FROM":
                in_from = True
            elif in_from and token.ttype not in (Keyword, sqlparse.tokens.Whitespace, sqlparse.tokens.Punctuation):
                table_name = self._clean_identifier(token.value)
                if table_name and not self._is_subquery_alias(table_name):
                    self.read_tables.add(table_name)
                in_from = False
            
            # Find JOIN tables
            if token.ttype is Keyword and "JOIN" in token_text:
                in_join = True
            elif in_join and token.ttype not in (Keyword, sqlparse.tokens.Whitespace):
                table_name = self._clean_identifier(token.value)
                if table_name:
                    self.read_tables.add(table_name)
                in_join = False
    
    def _parse_select(self, statement: sqlparse.sql.Statement) -> None:
        """Parse SELECT statement."""
        tokens = list(statement.flatten())
        
        in_from = False
        in_join = False
        
        for i, token in enumerate(tokens):
            token_text = token.value.upper()
            
            if token.ttype is Keyword and token_text == "FROM":
                in_from = True
            elif in_from and token.ttype not in (Keyword, sqlparse.tokens.Whitespace, sqlparse.tokens.Punctuation):
                table_name = self._clean_identifier(token.value)
                if table_name and not self._is_subquery_alias(table_name):
                    self.read_tables.add(table_name)
                in_from = False
            
            if token.ttype is Keyword and "JOIN" in token_text:
                in_join = True
            elif in_join and token.ttype not in (Keyword, sqlparse.tokens.Whitespace):
                table_name = self._clean_identifier(token.value)
                if table_name:
                    self.read_tables.add(table_name)
                in_join = False
    
    def _parse_create(self, statement: sqlparse.sql.Statement) -> None:
        """Parse CREATE statement."""
        sql = statement.value.upper()
        
        # CREATE TABLE
        create_table_match = re.search(
            r"CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)",
            sql
        )
        if create_table_match:
            table_name = self._clean_identifier(create_table_match.group(1))
            self.write_tables.add(table_name)
        
        # CREATE TABLE AS SELECT
        if "AS SELECT" in sql or re.search(r"AS\s+SELECT", sql):
            self._parse_select(statement)
        
        # LOCATION clause
        location_match = re.search(r"LOCATION\s+['\"]([^'\"]+)['\"]", sql)
        if location_match:
            self.paths.add(location_match.group(1))
    
    def _parse_dml(self, statement: sqlparse.sql.Statement) -> None:
        """Parse DELETE/UPDATE statement."""
        tokens = list(statement.flatten())
        
        for i, token in enumerate(tokens):
            if token.ttype is Keyword and token.value.upper() in ("FROM", "UPDATE"):
                # Next non-keyword token is the table
                for j in range(i + 1, len(tokens)):
                    if tokens[j].ttype not in (Keyword, sqlparse.tokens.Whitespace):
                        table_name = self._clean_identifier(tokens[j].value)
                        self.write_tables.add(table_name)
                        break
    
    def _parse_with_regex(self, sql: str) -> None:
        """Fallback regex-based parsing."""
        sql_upper = sql.upper()
        
        # LOAD DATA INPATH
        load_match = re.search(
            r"LOAD\s+DATA\s+(?:LOCAL\s+)?INPATH\s+['\"]([^'\"]+)['\"]\s+INTO\s+TABLE\s+([^\s;]+)",
            sql_upper
        )
        if load_match:
            self.paths.add(load_match.group(1))
            self.write_tables.add(self._clean_identifier(load_match.group(2)))
        
        # ALTER TABLE SET LOCATION
        alter_match = re.search(
            r"ALTER\s+TABLE\s+([^\s]+)\s+SET\s+LOCATION\s+['\"]([^'\"]+)['\"]",
            sql_upper
        )
        if alter_match:
            self.write_tables.add(self._clean_identifier(alter_match.group(1)))
            self.paths.add(alter_match.group(2))
        
        # CREATE VIEW
        view_match = re.search(
            r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([^\s(]+)",
            sql_upper
        )
        if view_match:
            self.write_tables.add(self._clean_identifier(view_match.group(1)))
    
    def _clean_identifier(self, identifier: str) -> str:
        """Clean table/column identifier."""
        # Remove backticks, quotes, trailing commas/semicolons
        identifier = identifier.strip("`'\"(); \t\n")
        # Handle database.table notation
        if "." in identifier:
            parts = identifier.split(".")
            return ".".join(p.strip() for p in parts)
        return identifier
    
    def _is_subquery_alias(self, name: str) -> bool:
        """Check if name looks like a subquery alias."""
        # Simple heuristic: single letter or very short names are likely aliases
        return len(name) <= 2 and name.isalpha()


class HiveExtractor(BaseExtractor):
    """Extractor for Hive SQL files."""
    
    def __init__(self, rule_engine: Optional[RuleEngine] = None) -> None:
        super().__init__()
        self.rule_engine = rule_engine
        self.parser = HiveSQLParser()
    
    def extract(self, file_path: Path) -> List[Fact]:
        """Extract facts from Hive SQL file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            return self.extract_from_content(content, str(file_path))
        
        except Exception as e:
            print(f"Error extracting from {file_path}: {e}")
            return []
    
    def extract_from_content(self, content: str, source_file: str) -> List[Fact]:
        """Extract facts from Hive SQL content."""
        facts = []
        
        # Remove comments
        content_no_comments = self._remove_comments(content)
        
        # Split into statements (rough split on semicolons)
        statements = self._split_statements(content_no_comments)
        
        for stmt_num, statement in enumerate(statements, 1):
            if not statement.strip():
                continue
            
            try:
                # Parse statement
                result = self.parser.parse_statement(statement)
                
                # Create read facts
                for table in result["read_tables"]:
                    fact = ReadFact(
                        source_file=source_file,
                        line_number=self._estimate_line_number(content, statement),
                        dataset_urn=f"hive://{table}",
                        dataset_type="hive",
                        confidence=0.85,
                        extraction_method=ExtractionMethod.SQL_PARSE,
                        evidence=statement[:200],
                        has_placeholders="${" in table
                    )
                    fact.params["table_name"] = table
                    facts.append(fact)
                
                # Create write facts
                for table in result["write_tables"]:
                    fact = WriteFact(
                        source_file=source_file,
                        line_number=self._estimate_line_number(content, statement),
                        dataset_urn=f"hive://{table}",
                        dataset_type="hive",
                        confidence=0.85,
                        extraction_method=ExtractionMethod.SQL_PARSE,
                        evidence=statement[:200],
                        has_placeholders="${" in table
                    )
                    fact.params["table_name"] = table
                    facts.append(fact)
                
                # Create path facts
                for path in result["paths"]:
                    fact = Fact(
                        source_file=source_file,
                        line_number=self._estimate_line_number(content, statement),
                        dataset_urn=path,
                        dataset_type="hdfs",
                        confidence=0.80,
                        extraction_method=ExtractionMethod.SQL_PARSE,
                        evidence=statement[:200],
                        has_placeholders="${" in path
                    )
                    facts.append(fact)
            
            except Exception as e:
                print(f"Error parsing statement in {source_file}: {e}")
        
        # Fallback to regex-based extraction if rule engine available
        if self.rule_engine:
            matches = self.rule_engine.apply_rules(content, "hive")
            for match in matches:
                fact = self._match_to_fact(match, source_file)
                if fact:
                    facts.append(fact)
        
        return facts
    
    def _remove_comments(self, content: str) -> str:
        """Remove SQL comments."""
        # Remove single-line comments
        content = re.sub(r"--.*?$", "", content, flags=re.MULTILINE)
        # Remove multi-line comments
        content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
        return content
    
    def _split_statements(self, content: str) -> List[str]:
        """Split SQL content into statements."""
        # Simple split on semicolons (not perfect but good enough)
        statements = content.split(";")
        return [stmt.strip() for stmt in statements if stmt.strip()]
    
    def _estimate_line_number(self, full_content: str, statement: str) -> int:
        """Estimate line number of statement in full content."""
        try:
            index = full_content.index(statement[:50])
            return full_content[:index].count("\n") + 1
        except ValueError:
            return 0
    
    def _match_to_fact(self, match: dict, source_file: str) -> Optional[Fact]:
        """Convert rule match to fact."""
        from lineage.rules import RuleAction
        
        action = match["action"]
        
        if action == RuleAction.READ_HIVE_TABLE:
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
            fact = WriteFact(
                source_file=source_file,
                dataset_urn=f"hive://{table}",
                dataset_type="hive",
                confidence=match["confidence"],
                extraction_method=ExtractionMethod.REGEX,
                evidence=match["match_text"],
                has_placeholders="${" in table
            )
            fact.params["table_name"] = table
            return fact
        
        return None
    
    def get_confidence_base(self) -> float:
        """Get base confidence for SQL parsing."""
        return 0.85

