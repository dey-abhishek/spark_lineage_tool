"""Scala Spark extractor using pattern matching."""

from pathlib import Path
from typing import List, Optional, Set
from datetime import datetime
import re
import sqlparse
from sqlparse.sql import Token
from sqlparse.tokens import Keyword

from .base import BaseExtractor
from lineage.ir import Fact, ReadFact, WriteFact, ConfigFact, ExtractionMethod
from lineage.rules import RuleEngine
from .method_call_tracker import ScalaMethodCallTracker


class ScalaExtractor(BaseExtractor):
    """Extractor for Scala Spark files.
    
    Uses regex-based pattern matching. For production, consider using tree-sitter-scala
    or calling a Scala-based parser via subprocess.
    """
    
    def __init__(self, rule_engine: Optional[RuleEngine] = None) -> None:
        super().__init__()
        self.rule_engine = rule_engine
        
        # Scala-specific patterns - allow for intermediate method calls
        self.patterns = {
            # Handle both regular strings and string interpolation
            # Allow for .read followed eventually by .parquet/csv/etc
            "read_parquet": re.compile(r'\.read(?:\.\w+\([^)]*\))*\.parquet\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "read_csv": re.compile(r'\.read(?:\.\w+\([^)]*\))*\.csv\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "read_json": re.compile(r'\.read(?:\.\w+\([^)]*\))*\.json\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "read_orc": re.compile(r'\.read(?:\.\w+\([^)]*\))*\.orc\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "read_table": re.compile(r'\.table\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            # Format with load pattern for Delta
            "read_format_load": re.compile(r'\.read\.format\("([^"]+)"\)(?:\.\w+\([^)]*\))*\.load\((?:s?)"([^"]+)"\)', re.DOTALL),
            "write_format_save": re.compile(r'\.write(?:\.\w+\([^)]*\))*\.format\("([^"]+)"\)(?:\.\w+\([^)]*\))*\.save\((?:s?)"([^"]+)"\)', re.DOTALL),
            "write_parquet": re.compile(r'\.write(?:\.\w+\([^)]*\))*\.parquet\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "write_csv": re.compile(r'\.write(?:\.\w+\([^)]*\))*\.csv\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "write_json": re.compile(r'\.write(?:\.\w+\([^)]*\))*\.json\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "write_orc": re.compile(r'\.write(?:\.\w+\([^)]*\))*\.orc\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "save_as_table": re.compile(r'\.saveAsTable\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "insert_into": re.compile(r'\.insertInto\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "spark_sql": re.compile(r'\.sql\((?:s?)"([^"]+)"\)', re.DOTALL),
            # JDBC patterns - handle both single and triple-quoted strings
            "jdbc_read": re.compile(r'\.read\.jdbc\([^,]+,\s*(?:s?)(?:"""([^"]+(?:"[^"]|""[^"])*?)"""|"([^"]+)")', re.DOTALL),
            "jdbc_write": re.compile(r'\.write\.jdbc\([^,]+,\s*(?:s?)(?:"""([^"]+(?:"[^"]|""[^"])*?)"""|"([^"]+)")', re.DOTALL),
            # Kafka patterns - Spark structured streaming
            "kafka_read_stream": re.compile(r'\.readStream\.format\("kafka"\).*?\.option\("subscribe",\s*"([^"]+)"\)', re.DOTALL),
            "kafka_write_stream": re.compile(r'\.writeStream\.format\("kafka"\).*?\.option\("topic",\s*"([^"]+)"\)', re.DOTALL),
            # Kafka patterns - Native Kafka producer/consumer
            "kafka_producer_record": re.compile(r'new\s+ProducerRecord\[.*?\]\(\s*"([^"]+)"', re.DOTALL),
            "kafka_consumer_subscribe": re.compile(r'\.subscribe\(.*?Collections\.singletonList\(\s*"([^"]+)"\)', re.DOTALL),
            "kafka_consumer_subscribe_var": re.compile(r'\.subscribe\(.*?Collections\.singletonList\(\s*(\w+)\s*\)', re.DOTALL),
        }
    
    def extract(self, file_path: Path) -> List[Fact]:
        """Extract facts from Scala file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            return self.extract_from_content(content, str(file_path))
        
        except Exception as e:
            print(f"Error extracting from {file_path}: {e}")
            return []
    
    def extract_from_content(self, content: str, source_file: str) -> List[Fact]:
        """Extract facts from Scala content."""
        facts = []
        
        # Remove comments
        content_no_comments = self._remove_comments(content)
        
        # Extract variable definitions (val/var varName = "value")
        facts.extend(self._extract_variable_definitions(content_no_comments, source_file))
        
        # Inter-procedural analysis: extract method definitions and calls
        method_tracker = ScalaMethodCallTracker()
        method_tracker.extract_methods(content_no_comments)
        method_tracker.extract_calls(content_no_comments)
        
        # Get resolved method bodies (with arguments substituted)
        resolved_bodies = method_tracker.get_tracker().get_all_resolved_bodies()
        
        # Extract facts from resolved method bodies
        for call, resolved_body in resolved_bodies:
            facts.extend(self._extract_from_resolved_body(resolved_body, source_file, call.line_number))
        
        # Join lines for method chains (normalize whitespace around dots and newlines)
        content_joined = re.sub(r'\s*\n\s*\.', '.', content_no_comments)
        # Also normalize spaces around dots
        content_joined = re.sub(r'\s*\.\s*', '.', content_joined)
        
        # Extract read operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("read_") and pattern_name != "read_format_load":
                for match in pattern.finditer(content_joined):
                    # Handle both capture groups: quoted string or variable name
                    path = match.group(1) if match.group(1) else match.group(2)
                    if not path:
                        continue
                    
                    line_number = content[:match.start()].count("\n") + 1
                    
                    # Check for string interpolation - look for $ in the path
                    # OR if path is a variable name (not starting with /)
                    has_placeholders = "$" in path or (not path.startswith("/") and not path.startswith("hive://"))
                    
                    if "table" in pattern_name:
                        dataset_type = "hive"
                        urn = f"hive://{path}"
                    else:
                        dataset_type = "hdfs"
                        urn = path
                    
                    fact = ReadFact(
                        source_file=source_file,
                        line_number=line_number,
                        dataset_urn=urn,
                        dataset_type=dataset_type,
                        confidence=0.75,
                        extraction_method=ExtractionMethod.REGEX,
                        evidence=match.group(0)[:100],
                        has_placeholders=has_placeholders
                    )
                    facts.append(fact)
            
            # Special handling for .format().load() pattern
            elif pattern_name == "read_format_load":
                for match in pattern.finditer(content_joined):
                    format_type = match.group(1)
                    path = match.group(2)
                    line_number = content[:match.start()].count("\n") + 1
                    
                    has_placeholders = "$" in path
                    
                    # Determine dataset type based on format
                    if format_type == "delta":
                        dataset_type = "delta"
                    elif "sftp" in format_type.lower():
                        dataset_type = "sftp"
                    else:
                        dataset_type = "hdfs"
                    
                    fact = ReadFact(
                        source_file=source_file,
                        line_number=line_number,
                        dataset_urn=path,
                        dataset_type=dataset_type,
                        confidence=0.75,
                        extraction_method=ExtractionMethod.REGEX,
                        evidence=match.group(0)[:100],
                        has_placeholders=has_placeholders
                    )
                    fact.params["format"] = format_type
                    facts.append(fact)
        
        # Extract write operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("write_") or pattern_name in ["save_as_table", "insert_into"]:
                for match in pattern.finditer(content_joined):
                    # Special handling for write_format_save pattern (has 2 groups: format, path)
                    if pattern_name == "write_format_save":
                        format_type = match.group(1)
                        path = match.group(2)
                        if not path:
                            continue
                        
                        line_number = content[:match.start()].count("\n") + 1
                        has_placeholders = "$" in path
                        
                        # Determine dataset type based on format
                        if format_type == "delta":
                            dataset_type = "delta"
                        elif "sftp" in format_type.lower():
                            dataset_type = "sftp"
                        else:
                            dataset_type = "hdfs"
                        
                        fact = WriteFact(
                            source_file=source_file,
                            line_number=line_number,
                            dataset_urn=path,
                            dataset_type=dataset_type,
                            confidence=0.75,
                            extraction_method=ExtractionMethod.REGEX,
                            evidence=match.group(0)[:100],
                            has_placeholders=has_placeholders
                        )
                        fact.params["format"] = format_type
                        facts.append(fact)
                    else:
                        # Handle both capture groups: quoted string or variable name
                        path = match.group(1) if match.group(1) else match.group(2)
                        if not path:
                            continue
                        
                        line_number = content[:match.start()].count("\n") + 1
                        
                        # Check for string interpolation or variable reference
                        has_placeholders = "$" in path or (not path.startswith("/") and not path.startswith("hive://"))
                        
                        if pattern_name in ["save_as_table", "insert_into"]:
                            dataset_type = "hive"
                            urn = f"hive://{path}"
                        else:
                            dataset_type = "hdfs"
                            urn = path
                        
                        fact = WriteFact(
                            source_file=source_file,
                            line_number=line_number,
                            dataset_urn=urn,
                            dataset_type=dataset_type,
                            confidence=0.75,
                            extraction_method=ExtractionMethod.REGEX,
                            evidence=match.group(0),
                            has_placeholders=has_placeholders
                        )
                        facts.append(fact)
        
        # Extract JDBC operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name in ["jdbc_read", "jdbc_write"]:
                for match in pattern.finditer(content_joined):
                    # Handle both triple-quoted (group 1) and single-quoted (group 2) strings
                    table_or_query = match.group(1) if match.group(1) else match.group(2)
                    line_number = content[:match.start()].count("\n") + 1
                    
                    has_placeholders = "$" in table_or_query
                    dataset_type = "jdbc"
                    
                    if pattern_name == "jdbc_read":
                        # Extract actual table names from JDBC query
                        # Could be a simple table name or a SQL query in parentheses
                        tables = self._extract_tables_from_jdbc_query(table_or_query)
                        
                        # Create a fact for each extracted table
                        for table in tables:
                            fact = ReadFact(
                                source_file=source_file,
                                line_number=line_number,
                                dataset_urn=f"jdbc://{table}",
                                dataset_type=dataset_type,
                                confidence=0.80 if not table_or_query.startswith('(') else 0.70,  # Lower confidence for parsed queries
                                extraction_method=ExtractionMethod.REGEX,
                                evidence=match.group(0)[:100],
                                has_placeholders=has_placeholders or "$" in table
                            )
                            fact.params["dbtable"] = table
                            facts.append(fact)
                    else:
                        # JDBC write - typically a simple table name, but could also be a query
                        tables = self._extract_tables_from_jdbc_query(table_or_query)
                        
                        # Create a fact for each extracted table
                        for table in tables:
                            fact = WriteFact(
                                source_file=source_file,
                                line_number=line_number,
                                dataset_urn=f"jdbc://{table}",
                                dataset_type=dataset_type,
                                confidence=0.80 if not table_or_query.startswith('(') else 0.70,
                                extraction_method=ExtractionMethod.REGEX,
                                evidence=match.group(0)[:100],
                                has_placeholders=has_placeholders or "$" in table
                            )
                            fact.params["dbtable"] = table
                            facts.append(fact)
        
        # Extract Kafka operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("kafka_"):
                for match in pattern.finditer(content_joined):
                    topic = match.group(1)
                    line_number = content[:match.start()].count("\n") + 1
                    
                    has_placeholders = "$" in topic
                    dataset_type = "kafka"
                    urn = f"kafka://{topic}"
                    
                    if pattern_name in ["kafka_read_stream", "kafka_consumer_subscribe", "kafka_consumer_subscribe_var"]:
                        # This is a READ from Kafka
                        fact = ReadFact(
                            source_file=source_file,
                            line_number=line_number,
                            dataset_urn=urn,
                            dataset_type=dataset_type,
                            confidence=0.85 if not has_placeholders else 0.75,
                            extraction_method=ExtractionMethod.REGEX,
                            evidence=match.group(0)[:100],
                            has_placeholders=has_placeholders
                        )
                        fact.params["kafka_topic"] = topic
                        fact.params["streaming"] = True if "stream" in pattern_name else False
                        facts.append(fact)
                    elif pattern_name in ["kafka_write_stream", "kafka_producer_record"]:
                        # This is a WRITE to Kafka
                        fact = WriteFact(
                            source_file=source_file,
                            line_number=line_number,
                            dataset_urn=urn,
                            dataset_type=dataset_type,
                            confidence=0.85 if not has_placeholders else 0.75,
                            extraction_method=ExtractionMethod.REGEX,
                            evidence=match.group(0)[:100],
                            has_placeholders=has_placeholders
                        )
                        fact.params["kafka_topic"] = topic
                        fact.params["streaming"] = True if "stream" in pattern_name else False
                        facts.append(fact)
        
        # Use rule engine for additional patterns
        if self.rule_engine:
            matches = self.rule_engine.apply_rules(content_joined, "scala")
            for match in matches:
                fact = self._match_to_fact(match, source_file)
                if fact:
                    facts.append(fact)
        
        return facts
    
    def _extract_variable_definitions(self, content: str, source_file: str) -> List[Fact]:
        """Extract variable definitions from Scala code (val/var name = "value")."""
        facts = []
        
        # Pattern 1: Simple string literals
        # Matches: val sourceDb = "prod_db", var env = "prod"
        var_pattern = re.compile(r'^\s*(?:val|var)\s+(\w+)\s*=\s*"([^"]+)"', re.MULTILINE)
        
        for match in var_pattern.finditer(content):
            var_name = match.group(1)
            value = match.group(2).strip()
            line_number = content[:match.start()].count("\n") + 1
            
            # Create a ConfigFact for the variable definition
            fact = ConfigFact(
                source_file=source_file,
                line_number=line_number,
                config_key=var_name,
                config_value=value,
                config_source="scala_assignment",
                extraction_method=ExtractionMethod.REGEX,
                confidence=0.90
            )
            facts.append(fact)
        
        # Pattern 2: Conditional with default value (common pattern)
        # Matches: val runDate = if (args.length > 0) args(0) else "2024-01-01"
        #          val env = if (args.length > 1) args(1) else "prod"
        conditional_pattern = re.compile(
            r'(?:val|var)\s+(\w+)\s*=\s*if.*?else\s*"([^"]+)"',
            re.DOTALL
        )
        
        for match in conditional_pattern.finditer(content):
            var_name = match.group(1)
            default_value = match.group(2).strip()
            line_number = content[:match.start()].count("\n") + 1
            
            # Create a ConfigFact for the default value
            fact = ConfigFact(
                source_file=source_file,
                line_number=line_number,
                config_key=var_name,
                config_value=default_value,
                config_source="scala_conditional_default",
                extraction_method=ExtractionMethod.REGEX,
                confidence=0.85  # Slightly lower since it's conditional
            )
            facts.append(fact)
        
        # Pattern 3: DateTime formatting patterns
        # Matches: val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
        #          val timestamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date())
        # For static analysis, we resolve to current timestamp
        datetime_patterns = [
            # LocalDateTime patterns
            (r'(?:val|var)\s+(\w+)\s*=\s*LocalDateTime\.now\(\)\.format\([^"]*"([^"]+)"', 'localdatetime'),
            # SimpleDateFormat patterns (assignment on different line)
            (r'(?:val|var)\s+(\w+)\s*=\s*new\s+SimpleDateFormat\("([^"]+)"\)\.format', 'simpledateformat'),
            # SimpleDateFormat patterns (variable then format)
            (r'(?:val|var)\s+(\w+)\s*=\s*\w+\.format\([^)]*\)', 'simpledateformat_var'),
        ]
        
        from datetime import datetime
        now = datetime.now()
        
        for pattern_regex, pattern_type in datetime_patterns:
            pattern = re.compile(pattern_regex, re.DOTALL)
            for match in pattern.finditer(content):
                var_name = match.group(1)
                format_str = match.group(2).strip()
                line_number = content[:match.start()].count("\n") + 1
                
                # Convert Java/Scala date format to Python strftime format
                resolved_value = self._resolve_datetime_format(format_str, now)
                
                fact = ConfigFact(
                    source_file=source_file,
                    line_number=line_number,
                    config_key=var_name,
                    config_value=resolved_value,
                    config_source=f"scala_{pattern_type}",
                    extraction_method=ExtractionMethod.REGEX,
                    confidence=0.88
                )
                facts.append(fact)
        
        return facts
    
    def _extract_from_resolved_body(self, resolved_body: str, source_file: str, line_number: int) -> List[Fact]:
        """Extract facts from a resolved method body (with arguments substituted).
        
        This allows us to extract Spark API calls from custom wrapper methods.
        """
        facts = []
        
        # Normalize the resolved body
        resolved_body = re.sub(r'\s*\n\s*\.', '.', resolved_body)
        resolved_body = re.sub(r'\s*\.\s*', '.', resolved_body)
        
        # Try all read patterns
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("read_") and pattern_name != "read_format_load":
                for match in pattern.finditer(resolved_body):
                    path = match.group(1) if match.group(1) else match.group(2)
                    if not path:
                        continue
                    
                    has_placeholders = "$" in path or (not path.startswith("/") and not path.startswith("hive://"))
                    
                    if "table" in pattern_name:
                        dataset_type = "hive"
                        urn = f"hive://{path}"
                    else:
                        dataset_type = "hdfs"
                        urn = path
                    
                    fact = ReadFact(
                        source_file=source_file,
                        line_number=line_number,
                        dataset_urn=urn,
                        dataset_type=dataset_type,
                        confidence=0.80,  # Slightly lower confidence for inter-procedural
                        extraction_method=ExtractionMethod.REGEX,
                        evidence=f"Resolved from custom wrapper call",
                        has_placeholders=has_placeholders
                    )
                    facts.append(fact)
            elif pattern_name == "read_format_load":
                for match in pattern.finditer(resolved_body):
                    format_type = match.group(1)
                    path = match.group(2)
                    if not path:
                        continue
                    
                    has_placeholders = "$" in path
                    
                    fact = ReadFact(
                        source_file=source_file,
                        line_number=line_number,
                        dataset_urn=path,
                        dataset_type="hdfs",
                        confidence=0.80,
                        extraction_method=ExtractionMethod.REGEX,
                        evidence=f"Resolved from custom wrapper call",
                        has_placeholders=has_placeholders
                    )
                    facts.append(fact)
        
        # Try all write patterns
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("write_") and pattern_name != "write_format_save" or pattern_name in ("save_as_table", "insert_into"):
                for match in pattern.finditer(resolved_body):
                    path = match.group(1) if match.group(1) else match.group(2)
                    if not path:
                        continue
                    
                    has_placeholders = "$" in path or (not path.startswith("/") and not path.startswith("hive://"))
                    
                    if "table" in pattern_name or pattern_name in ("save_as_table", "insert_into"):
                        dataset_type = "hive"
                        urn = f"hive://{path}"
                    else:
                        dataset_type = "hdfs"
                        urn = path
                    
                    fact = WriteFact(
                        source_file=source_file,
                        line_number=line_number,
                        dataset_urn=urn,
                        dataset_type=dataset_type,
                        confidence=0.80,  # Slightly lower confidence for inter-procedural
                        extraction_method=ExtractionMethod.REGEX,
                        evidence=f"Resolved from custom wrapper call",
                        has_placeholders=has_placeholders
                    )
                    facts.append(fact)
            elif pattern_name == "write_format_save":
                for match in pattern.finditer(resolved_body):
                    format_type = match.group(1)
                    path = match.group(2)
                    if not path:
                        continue
                    
                    has_placeholders = "$" in path
                    
                    fact = WriteFact(
                        source_file=source_file,
                        line_number=line_number,
                        dataset_urn=path,
                        dataset_type="hdfs",
                        confidence=0.80,
                        extraction_method=ExtractionMethod.REGEX,
                        evidence=f"Resolved from custom wrapper call",
                        has_placeholders=has_placeholders
                    )
                    facts.append(fact)
        
        return facts
    
    def _resolve_datetime_format(self, java_format: str, dt: datetime) -> str:
        """Convert Java/Scala date format to actual value.
        
        Java/Scala formats:
            yyyy-MM-dd -> 2024-01-15
            yyyyMMddHHmmss -> 20240115143025
            yyyy-MM-dd_HH-mm-ss -> 2024-01-15_14-30-25
        """
        # Map Java format characters to Python strftime
        format_map = {
            'yyyy': '%Y',
            'yy': '%y',
            'MM': '%m',
            'dd': '%d',
            'HH': '%H',
            'mm': '%M',
            'ss': '%S',
        }
        
        python_format = java_format
        for java_fmt, python_fmt in format_map.items():
            python_format = python_format.replace(java_fmt, python_fmt)
        
        try:
            return dt.strftime(python_format)
        except:
            # If conversion fails, return placeholder
            return f"TIMESTAMP_{java_format}"
    
    def _extract_tables_from_jdbc_query(self, query: str) -> Set[str]:
        """Extract table names from JDBC query string.
        
        JDBC queries can be:
        1. Simple table name: "customers"
        2. SQL query in parentheses: "(SELECT * FROM customers WHERE ...)"
        """
        tables = set()
        
        # Check if query is wrapped in parentheses (SQL subquery)
        query_stripped = query.strip()
        if query_stripped.startswith('(') and query_stripped.endswith(')'):
            # This is a SQL query, parse it to extract table names
            sql_query = query_stripped[1:-1]  # Remove outer parentheses
            
            try:
                parsed = sqlparse.parse(sql_query)
                for statement in parsed:
                    # Extract tables from FROM clauses
                    tables.update(self._extract_tables_from_select(statement))
            except Exception:
                # If parsing fails, try regex fallback
                # Look for FROM <table> or JOIN <table>
                from_pattern = re.compile(r'\bFROM\s+([^\s,;()]+)', re.IGNORECASE)
                join_pattern = re.compile(r'\bJOIN\s+([^\s,;()]+)', re.IGNORECASE)
                
                for match in from_pattern.finditer(sql_query):
                    table_name = match.group(1).strip()
                    if table_name and not table_name.upper() in ('SELECT', 'WHERE'):
                        tables.add(table_name)
                
                for match in join_pattern.finditer(sql_query):
                    table_name = match.group(1).strip()
                    if table_name and not table_name.upper() in ('SELECT', 'WHERE'):
                        tables.add(table_name)
        else:
            # Simple table name
            tables.add(query_stripped)
        
        return tables
    
    def _extract_tables_from_select(self, statement: sqlparse.sql.Statement) -> Set[str]:
        """Extract table names from SELECT statement using sqlparse."""
        tables = set()
        tokens = list(statement.flatten())
        
        in_from = False
        in_join = False
        
        for i, token in enumerate(tokens):
            token_text = token.value.upper()
            
            if token.ttype is Keyword and token_text == "FROM":
                in_from = True
            elif in_from and token.ttype not in (Keyword, sqlparse.tokens.Whitespace, sqlparse.tokens.Punctuation):
                table_name = self._extract_sql_identifier(tokens, i)
                if table_name:
                    tables.add(table_name)
                in_from = False
            
            if token.ttype is Keyword and "JOIN" in token_text:
                in_join = True
            elif in_join and token.ttype not in (Keyword, sqlparse.tokens.Whitespace):
                table_name = self._extract_sql_identifier(tokens, i)
                if table_name:
                    tables.add(table_name)
                in_join = False
        
        return tables
    
    def _extract_sql_identifier(self, tokens: List, start_idx: int) -> str:
        """Extract SQL identifier (table name) from token list."""
        identifier_parts = []
        i = start_idx
        
        while i < len(tokens):
            token = tokens[i]
            value = token.value
            ttype = token.ttype
            
            # Stop at keywords
            if ttype == Keyword:
                break
            
            # Stop at whitespace
            if ttype == sqlparse.tokens.Whitespace:
                break
            
            # Include dots for schema.table notation
            if ttype == sqlparse.tokens.Punctuation:
                if value == '.':
                    identifier_parts.append(value)
                    i += 1
                    continue
                else:
                    break
            
            # Stop at SQL keywords
            if value.upper() in ('WHERE', 'ON', 'AND', 'OR', 'SELECT', 'FROM', 'JOIN', 
                                  'LEFT', 'RIGHT', 'INNER', 'OUTER', 'GROUP', 'ORDER', 'LIMIT'):
                break
            
            identifier_parts.append(value)
            i += 1
        
        return ''.join(identifier_parts).strip()
    
    def _remove_comments(self, content: str) -> str:
        """Remove Scala comments."""
        # Remove single-line comments
        content = re.sub(r"//.*?$", "", content, flags=re.MULTILINE)
        # Remove multi-line comments
        content = re.sub(r"/\*.*?\*/", "", content, flags=re.DOTALL)
        return content
    
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
                has_placeholders="${" in path
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
                has_placeholders="${" in path
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
        """Get base confidence for Scala extraction."""
        return 0.70

