"""PySpark AST-based extractor."""

import ast
from pathlib import Path
from typing import List, Optional, Any, Set
import re
import sqlparse

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
        
        # Detect Delta operations (DeltaTable.forPath, .merge)
        if self._is_delta_operation(call_chain):
            fact = self._extract_delta_fact(node, call_chain)
            if fact:
                self.facts.append(fact)
        
        # Detect read operations
        elif self._is_read_operation(call_chain):
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
        
        # Detect JDBC operations
        elif self._is_jdbc_operation(call_chain):
            facts = self._extract_jdbc_facts(node, call_chain)
            if facts:
                self.facts.extend(facts)
        
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
        # Handle both batch and streaming reads
        return ("read" in chain or "readStream" in chain) and any(method in chain for method in read_methods)
    
    def _is_write_operation(self, chain: List[str]) -> bool:
        """Check if call chain is a write operation."""
        write_methods = ["parquet", "csv", "json", "orc", "text", "avro", "save", "start"]
        # Handle both batch and streaming writes
        # For streaming: writeStream.format().start() or writeStream.format().option().start()
        return ("write" in chain or "writeStream" in chain) and any(method in chain for method in write_methods)
    
    def _is_table_operation(self, chain: List[str]) -> bool:
        """Check if call chain is a table operation."""
        table_methods = ["table", "saveAsTable", "insertInto"]
        return any(method in chain for method in table_methods)
    
    def _is_sql_operation(self, chain: List[str]) -> bool:
        """Check if call chain is a SQL operation."""
        return "sql" in chain
    
    def _is_delta_operation(self, chain: List[str]) -> bool:
        """Check if call chain is a Delta Lake operation."""
        delta_methods = ["forPath", "forName", "merge"]
        return "DeltaTable" in chain or any(method in chain for method in delta_methods)
    
    def _is_jdbc_operation(self, chain: List[str]) -> bool:
        """Check if call chain is a JDBC operation."""
        return "jdbc" in chain or ("read" in chain and "format" in chain)
    
    def _extract_string_arg(self, node: ast.Call, pos: int = 0) -> Optional[str]:
        """Extract string argument from call."""
        if len(node.args) > pos:
            arg = node.args[pos]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                return arg.value
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
            elif isinstance(value, ast.FormattedValue):
                if isinstance(value.value, ast.Name):
                    parts.append(f"${{{value.value.id}}}")
                else:
                    parts.append("${...}")
        return "".join(parts)
    
    def _extract_format_from_options(self, node: ast.Call) -> Optional[str]:
        """Extract format from .format() or .option() calls in chain."""
        # Traverse the entire call chain looking for .format()
        current = node
        
        while current:
            if isinstance(current, ast.Call):
                # Check if this is a .format() call
                if isinstance(current.func, ast.Attribute) and current.func.attr == "format":
                    if len(current.args) > 0 and isinstance(current.args[0], ast.Constant):
                        return current.args[0].value
                
                # Move up the chain - check the value of the attribute
                if isinstance(current.func, ast.Attribute):
                    current = current.func.value
                else:
                    break
            else:
                break
        
        return None
    
    def _extract_option_value(self, node: ast.Call, option_name: str) -> Optional[str]:
        """Extract value from .option(name, value) calls in chain."""
        current = node
        
        while current:
            if isinstance(current, ast.Call):
                # Check if this is an .option() call
                if isinstance(current.func, ast.Attribute) and current.func.attr == "option":
                    if len(current.args) >= 2:
                        # Check if first arg matches option_name
                        if isinstance(current.args[0], ast.Constant) and current.args[0].value == option_name:
                            if isinstance(current.args[1], ast.Constant) and isinstance(current.args[1].value, str):
                                return current.args[1].value
                
                # Move up the chain
                if isinstance(current.func, ast.Attribute):
                    current = current.func.value
                else:
                    break
            else:
                break
        
        return None
    
    def _extract_read_fact(self, node: ast.Call, chain: List[str]) -> Optional[Fact]:
        """Extract read fact from AST node."""
        # Check for format-based reads (e.g., .format("kafka").load())
        format_type = self._extract_format_from_options(node)
        
        path = self._extract_string_arg(node)
        
        # For streaming sources like Kafka, format is the key info
        if format_type and not path:
            path = f"stream:{format_type}"
            evidence_path = format_type
        elif format_type:
            evidence_path = f"{format_type}:{path}"
        elif path:
            evidence_path = path
        else:
            return None
        
        fact = ReadFact(
            source_file=self.source_file,
            line_number=node.lineno,
            dataset_urn=path if path else f"stream:{format_type}",
            dataset_type="stream" if "readStream" in chain else "hdfs",
            confidence=0.85,
            extraction_method=ExtractionMethod.AST,
            evidence=f"spark.{'readStream' if 'readStream' in chain else 'read'}.{f'format({format_type}).' if format_type else ''}{chain[-1]}({evidence_path})",
            has_placeholders="${" in path if path else False
        )
        
        # Determine format from chain or explicit format call
        if format_type:
            fact.params["format"] = format_type
        elif "parquet" in chain:
            fact.params["format"] = "parquet"
        elif "csv" in chain:
            fact.params["format"] = "csv"
        elif "json" in chain:
            fact.params["format"] = "json"
        elif "orc" in chain:
            fact.params["format"] = "orc"
        
        # Mark streaming
        if "readStream" in chain:
            fact.params["streaming"] = True
        
        return fact
    
    def _extract_write_fact(self, node: ast.Call, chain: List[str]) -> Optional[Fact]:
        """Extract write fact from AST node."""
        # Check for format-based writes
        format_type = self._extract_format_from_options(node)
        
        # Try to extract path from arguments or options
        path = self._extract_string_arg(node)
        
        # For writeStream, check for path in .option("path", "...")
        if not path and "writeStream" in chain:
            path = self._extract_option_value(node, "path")
        
        if not path:
            return None
        
        fact = WriteFact(
            source_file=self.source_file,
            line_number=node.lineno,
            dataset_urn=path,
            dataset_type="stream" if "writeStream" in chain else "hdfs",
            confidence=0.85,
            extraction_method=ExtractionMethod.AST,
            evidence=f"df.{'writeStream' if 'writeStream' in chain else 'write'}.{chain[-1]}({path})",
            has_placeholders="${" in path
        )
        
        # Determine format
        if format_type:
            fact.params["format"] = format_type
        elif "parquet" in chain:
            fact.params["format"] = "parquet"
        elif "csv" in chain:
            fact.params["format"] = "csv"
        elif "json" in chain:
            fact.params["format"] = "json"
        elif "orc" in chain:
            fact.params["format"] = "orc"
        
        # Mark streaming
        if "writeStream" in chain:
            fact.params["streaming"] = True
            # Extract checkpoint location
            checkpoint = self._extract_option_value(node, "checkpointLocation")
            if checkpoint:
                # Create a separate fact for checkpoint
                checkpoint_fact = WriteFact(
                    source_file=self.source_file,
                    line_number=node.lineno,
                    dataset_urn=checkpoint,
                    dataset_type="hdfs",
                    confidence=0.85,
                    extraction_method=ExtractionMethod.AST,
                    evidence=f"checkpointLocation: {checkpoint}",
                    has_placeholders="${" in checkpoint
                )
                checkpoint_fact.params["is_checkpoint"] = True
                self.facts.append(checkpoint_fact)
        
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
        tables_read, tables_written = self._parse_sql_tables(sql)
        
        # Create facts for each table read
        for table in tables_read:
            read_fact = ReadFact(
                source_file=self.source_file,
                line_number=node.lineno,
                dataset_urn=f"hive://{table}",
                dataset_type="hive",
                confidence=0.80,
                extraction_method=ExtractionMethod.SQL_PARSE,
                evidence=f"spark.sql: FROM {table}",
                has_placeholders="${" in table
            )
            read_fact.params["sql"] = sql[:200]  # Truncate for storage
            read_fact.params["embedded"] = True
            read_fact.params["table_name"] = table
            self.facts.append(read_fact)
        
        # Create facts for each table written
        for table in tables_written:
            write_fact = WriteFact(
                source_file=self.source_file,
                line_number=node.lineno,
                dataset_urn=f"hive://{table}",
                dataset_type="hive",
                confidence=0.80,
                extraction_method=ExtractionMethod.SQL_PARSE,
                evidence=f"spark.sql: INSERT INTO {table}",
                has_placeholders="${" in table
            )
            write_fact.params["sql"] = sql[:200]  # Truncate for storage
            write_fact.params["embedded"] = True
            write_fact.params["table_name"] = table
            self.facts.append(write_fact)
        
        # Return None since we've already added facts directly
        return None
    
    def _parse_sql_tables(self, sql: str) -> tuple[List[str], List[str]]:
        """Parse SQL to extract table names."""
        tables_read = []
        tables_written = []
        
        try:
            # Normalize SQL
            sql_upper = sql.upper()
            
            # Extract INSERT/CREATE targets
            insert_pattern = r'INSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?TABLE\s+([a-zA-Z_][\w.]*)'
            for match in re.finditer(insert_pattern, sql_upper):
                table = sql[match.start(1):match.end(1)].lower()
                tables_written.append(table)
            
            # Extract FROM clauses
            from_pattern = r'FROM\s+([a-zA-Z_][\w.]*)'
            for match in re.finditer(from_pattern, sql_upper):
                table = sql[match.start(1):match.end(1)].lower()
                if table not in tables_read:
                    tables_read.append(table)
            
            # Extract JOIN clauses
            join_pattern = r'JOIN\s+([a-zA-Z_][\w.]*)'
            for match in re.finditer(join_pattern, sql_upper):
                table = sql[match.start(1):match.end(1)].lower()
                if table not in tables_read:
                    tables_read.append(table)
            
        except Exception as e:
            # If parsing fails, return empty lists
            pass
        
        return tables_read, tables_written
    
    def _extract_delta_fact(self, node: ast.Call, chain: List[str]) -> Optional[Fact]:
        """Extract Delta Lake operation fact."""
        # DeltaTable.forPath(spark, path) or DeltaTable.forName(spark, table)
        if "forPath" in chain:
            # Extract path from second argument
            if len(node.args) >= 2:
                path_arg = node.args[1]
                if isinstance(path_arg, ast.Constant) and isinstance(path_arg.value, str):
                    path = path_arg.value
                elif isinstance(path_arg, ast.Name):
                    path = f"${{{path_arg.id}}}"
                else:
                    return None
                
                # forPath reads the Delta table
                fact = ReadFact(
                    source_file=self.source_file,
                    line_number=node.lineno,
                    dataset_urn=path,
                    dataset_type="delta",
                    confidence=0.90,
                    extraction_method=ExtractionMethod.AST,
                    evidence=f"DeltaTable.forPath('{path}')",
                    has_placeholders="${" in path
                )
                fact.params["format"] = "delta"
                fact.params["delta_operation"] = "forPath"
                return fact
        
        elif "forName" in chain:
            # Extract table name from second argument
            if len(node.args) >= 2:
                table_arg = node.args[1]
                if isinstance(table_arg, ast.Constant) and isinstance(table_arg.value, str):
                    table = table_arg.value
                    fact = ReadFact(
                        source_file=self.source_file,
                        line_number=node.lineno,
                        dataset_urn=f"hive://{table}",
                        dataset_type="delta",
                        confidence=0.90,
                        extraction_method=ExtractionMethod.AST,
                        evidence=f"DeltaTable.forName('{table}')",
                        has_placeholders=False
                    )
                    fact.params["format"] = "delta"
                    fact.params["delta_operation"] = "forName"
                    fact.params["table_name"] = table
                    return fact
        
        elif "merge" in chain:
            # .merge() operation - this is a write operation on the target table
            # The evidence should mention "delta"
            fact = WriteFact(
                source_file=self.source_file,
                line_number=node.lineno,
                dataset_urn="delta_merge_target",  # Will be resolved from context
                dataset_type="delta",
                confidence=0.75,
                extraction_method=ExtractionMethod.AST,
                evidence="DeltaTable.merge() operation",
                has_placeholders=False
            )
            fact.params["format"] = "delta"
            fact.params["delta_operation"] = "merge"
            return fact
        
        return None
    
    def _extract_jdbc_facts(self, node: ast.Call, chain: List[str]) -> List[Fact]:
        """Extract JDBC read/write operations."""
        facts = []
        
        # Check if this is spark.read.jdbc() or spark.read.format("jdbc").load()
        is_read = "read" in chain
        is_write = "write" in chain
        
        if is_read:
            # Extract JDBC URL and table
            url = None
            table = None
            
            # Try to extract from .jdbc(url, table, ...)
            if "jdbc" in chain:
                url = self._extract_string_arg(node, 0)
                table = self._extract_string_arg(node, 1)
            
            # Try to extract from .option("url", ...) and .option("dbtable", ...)
            if not url:
                url = self._extract_option_value(node, "url")
            if not table:
                table = self._extract_option_value(node, "dbtable")
                if not table:
                    table = self._extract_option_value(node, "query")
            
            if url or table:
                # Determine database type from JDBC URL
                dataset_type = "jdbc"
                if url:
                    if "oracle" in url.lower():
                        dataset_type = "oracle"
                    elif "postgresql" in url.lower() or "postgres" in url.lower():
                        dataset_type = "postgres"
                    elif "mysql" in url.lower():
                        dataset_type = "mysql"
                    elif "sqlserver" in url.lower() or "mssql" in url.lower():
                        dataset_type = "mssql"
                
                dataset_urn = f"{url}#{table}" if url and table else (table if table else url)
                
                fact = ReadFact(
                    source_file=self.source_file,
                    line_number=node.lineno,
                    dataset_urn=dataset_urn,
                    dataset_type=dataset_type,
                    confidence=0.85,
                    extraction_method=ExtractionMethod.AST,
                    evidence=f"spark.read.jdbc(url={url[:50] if url else 'N/A'}, table={table[:50] if table else 'N/A'})",
                    has_placeholders=("${" in dataset_urn if dataset_urn else False)
                )
                
                fact.params["jdbc_url"] = url if url else "unknown"
                fact.params["dbtable"] = table if table else "unknown"
                facts.append(fact)
        
        elif is_write:
            # Extract JDBC URL and table for writes
            url = self._extract_option_value(node, "url")
            table = self._extract_option_value(node, "dbtable")
            
            if not url:
                # Try to extract from .jdbc(url, table, ...)
                url = self._extract_string_arg(node, 0)
                table = self._extract_string_arg(node, 1)
            
            if url or table:
                dataset_type = "jdbc"
                if url:
                    if "oracle" in url.lower():
                        dataset_type = "oracle"
                    elif "postgresql" in url.lower() or "postgres" in url.lower():
                        dataset_type = "postgres"
                    elif "mysql" in url.lower():
                        dataset_type = "mysql"
                    elif "sqlserver" in url.lower() or "mssql" in url.lower():
                        dataset_type = "mssql"
                
                dataset_urn = f"{url}#{table}" if url and table else (table if table else url)
                
                fact = WriteFact(
                    source_file=self.source_file,
                    line_number=node.lineno,
                    dataset_urn=dataset_urn,
                    dataset_type=dataset_type,
                    confidence=0.85,
                    extraction_method=ExtractionMethod.AST,
                    evidence=f"df.write.jdbc(url={url[:50] if url else 'N/A'}, table={table[:50] if table else 'N/A'})",
                    has_placeholders=("${" in dataset_urn if dataset_urn else False)
                )
                
                fact.params["jdbc_url"] = url if url else "unknown"
                fact.params["dbtable"] = table if table else "unknown"
                facts.append(fact)
        
        return facts


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

