"""Java extractor for Spark lineage analysis.

Extracts lineage facts from Java files including:
- Spark Java API operations
- Java UDFs (Spark and Hive)
- Kafka producers/consumers
- HDFS FileSystem operations
- JDBC connections
"""

import re
from pathlib import Path
from typing import List, Optional

from .base import BaseExtractor
from lineage.ir import Fact, ReadFact, WriteFact, ConfigFact, ExtractionMethod
from lineage.rules import RuleEngine


class JavaExtractor(BaseExtractor):
    """Extractor for Java files.
    
    Uses regex-based pattern matching to extract lineage from Java code.
    Focuses on Spark Java API, UDFs, Kafka, and HDFS operations.
    """
    
    def __init__(self, rule_engine: Optional[RuleEngine] = None) -> None:
        super().__init__()
        self.rule_engine = rule_engine
        
        # Java-specific patterns
        self.patterns = {
            # Spark Java API - Reads
            "spark_read_parquet": re.compile(r'\.read\(\)\.parquet\("([^"]+)"'),
            "spark_read_csv": re.compile(r'\.read\(\)\.csv\("([^"]+)"'),
            "spark_read_json": re.compile(r'\.read\(\)\.json\("([^"]+)"'),
            "spark_read_orc": re.compile(r'\.read\(\)\.orc\("([^"]+)"'),
            "spark_read_text": re.compile(r'\.read\(\)\.text\("([^"]+)"'),
            "spark_read_table": re.compile(r'\.read\(\)\.table\("([^"]+)"'),
            "spark_table": re.compile(r'\.table\("([^"]+)"'),
            
            # Spark Java API - Writes
            "spark_write_parquet": re.compile(r'\.write\(\)(?:\.mode\([^)]*\))?\.parquet\("([^"]+)"'),
            "spark_write_csv": re.compile(r'\.write\(\)(?:\.mode\([^)]*\))?\.csv\("([^"]+)"'),
            "spark_write_json": re.compile(r'\.write\(\)(?:\.mode\([^)]*\))?\.json\("([^"]+)"'),
            "spark_write_orc": re.compile(r'\.write\(\)(?:\.mode\([^)]*\))?\.orc\("([^"]+)"'),
            "spark_write_text": re.compile(r'\.write\(\)(?:\.mode\([^)]*\))?\.text\("([^"]+)"'),
            "spark_save_as_table": re.compile(r'\.saveAsTable\("([^"]+)"'),
            "spark_insert_into": re.compile(r'\.insertInto\("([^"]+)"'),
            
            # Kafka - Native Java API
            "kafka_producer_record": re.compile(r'new ProducerRecord<[^>]*>\("([^"]+)"'),
            "kafka_consumer_subscribe": re.compile(r'\.subscribe\(Collections\.singletonList\("([^"]+)"\)'),
            "kafka_consumer_subscribe_array": re.compile(r'\.subscribe\(Arrays\.asList\("([^"]+)"'),
            
            # HDFS FileSystem operations
            "hdfs_copy_from_local": re.compile(r'\.copyFromLocalFile\([^,]+, new Path\("([^"]+)"\)'),
            "hdfs_copy_to_local": re.compile(r'\.copyToLocalFile\(new Path\("([^"]+)"\)'),
            "hdfs_move_from_local": re.compile(r'\.moveFromLocalFile\([^,]+, new Path\("([^"]+)"\)'),
            "hdfs_rename_src": re.compile(r'\.rename\(new Path\("([^"]+)"\), new Path\("([^"]+)"\)'),
            "hdfs_delete": re.compile(r'\.delete\(new Path\("([^"]+)"\)'),
            
            # JDBC
            "jdbc_read": re.compile(r'\.jdbc\([^,]+, "([^"]+)"'),
            "jdbc_write": re.compile(r'\.jdbc\([^,]+, "([^"]+)"'),
            
            # Spark SQL
            "spark_sql": re.compile(r'\.sql\("([^"]+)"\)'),
        }
    
    def extract(self, file_path: Path) -> List[Fact]:
        """Extract facts from Java file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            return self.extract_from_content(content, str(file_path))
        
        except Exception as e:
            print(f"Error extracting from {file_path}: {e}")
            return []
    
    def extract_from_content(self, content: str, source_file: str) -> List[Fact]:
        """Extract facts from Java content."""
        facts = []
        
        # Remove comments
        content_no_comments = self._remove_comments(content)
        
        # Extract variable definitions (String var = "value")
        var_facts = self._extract_variable_definitions(content_no_comments, source_file)
        facts.extend(var_facts)
        
        # Build variable map for resolution
        var_map = {fact.config_key: fact.config_value for fact in var_facts if hasattr(fact, 'config_key')}
        
        # Extract HDFS operations with variable resolution
        facts.extend(self._extract_hdfs_operations(content_no_comments, source_file, var_map))
        
        # Extract SQL templates with variable resolution
        facts.extend(self._extract_sql_templates(content_no_comments, source_file, var_map))
        
        # Normalize whitespace for method chains - join lines that continue with a dot
        # This handles patterns like:
        #   spark.read()
        #       .parquet("path")
        content_joined = re.sub(r'\)\s*\n\s*\.', ').', content_no_comments)
        content_joined = re.sub(r'\s+', ' ', content_joined)  # Collapse multiple spaces
        
        # Extract Spark read operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("spark_read_") or pattern_name == "spark_table":
                for match in pattern.finditer(content_joined):
                    path = match.group(1)
                    if not path:
                        continue
                    
                    # Line number from original content (approximate)
                    line_number = 1
                    has_placeholders = "$" in path or "{" in path
                    
                    if "table" in pattern_name:
                        dataset_type = "hive"
                        urn = f"hive:///{path}"
                    else:
                        dataset_type = "hdfs"
                        urn = path
                    
                    fact = ReadFact(
                        source_file=source_file,
                        line_number=line_number,
                        dataset_urn=urn,
                        dataset_type=dataset_type,
                        confidence=0.80,
                        extraction_method=ExtractionMethod.REGEX,
                        evidence=match.group(0)[:100],
                        has_placeholders=has_placeholders
                    )
                    facts.append(fact)
        
        # Extract Spark write operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("spark_write_") or pattern_name in ["spark_save_as_table", "spark_insert_into"]:
                for match in pattern.finditer(content_joined):
                    path = match.group(1)
                    if not path:
                        continue
                    
                    # Line number from original content (approximate)
                    line_number = 1
                    has_placeholders = "$" in path or "{" in path
                    
                    if pattern_name in ["spark_save_as_table", "spark_insert_into"]:
                        dataset_type = "hive"
                        urn = f"hive:///{path}"
                    else:
                        dataset_type = "hdfs"
                        urn = path
                    
                    fact = WriteFact(
                        source_file=source_file,
                        line_number=line_number,
                        dataset_urn=urn,
                        dataset_type=dataset_type,
                        confidence=0.80,
                        extraction_method=ExtractionMethod.REGEX,
                        evidence=match.group(0)[:100],
                        has_placeholders=has_placeholders
                    )
                    facts.append(fact)
        
        # Extract Kafka operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("kafka_"):
                for match in pattern.finditer(content_joined):
                    topic = match.group(1)
                    line_number = content[:match.start()].count("\n") + 1
                    has_placeholders = "$" in topic or "{" in topic
                    
                    urn = f"kafka:///{topic}"
                    dataset_type = "kafka"
                    
                    if "producer" in pattern_name:
                        # Kafka producer - WRITE
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
                        facts.append(fact)
                    elif "consumer" in pattern_name:
                        # Kafka consumer - READ
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
                        facts.append(fact)
        
        # Extract HDFS operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("hdfs_"):
                for match in pattern.finditer(content_joined):
                    line_number = content[:match.start()].count("\n") + 1
                    
                    if pattern_name == "hdfs_rename":
                        # Rename has source and target
                        source_path = match.group(1)
                        target_path = match.group(2)
                        
                        # Create READ fact for source
                        read_fact = ReadFact(
                            source_file=source_file,
                            line_number=line_number,
                            dataset_urn=source_path,
                            dataset_type="hdfs",
                            confidence=0.75,
                            extraction_method=ExtractionMethod.REGEX,
                            evidence=f"rename: {source_path} -> {target_path}",
                            has_placeholders="${" in source_path
                        )
                        facts.append(read_fact)
                        
                        # Create WRITE fact for target
                        write_fact = WriteFact(
                            source_file=source_file,
                            line_number=line_number,
                            dataset_urn=target_path,
                            dataset_type="hdfs",
                            confidence=0.75,
                            extraction_method=ExtractionMethod.REGEX,
                            evidence=f"rename: {source_path} -> {target_path}",
                            has_placeholders="${" in target_path
                        )
                        facts.append(write_fact)
                    
                    elif pattern_name == "hdfs_path_concat":
                        # Path concatenation - just note it, don't create facts
                        base_path = match.group(1)
                        suffix = match.group(2)
                        full_path = base_path + suffix
                        # Could be read or write, context needed
                        continue
                    
                    else:
                        # Single path operations
                        path = match.group(1)
                        has_placeholders = "$" in path or "{" in path
                        
                        if "copy_to_local" in pattern_name or "delete" in pattern_name:
                            # READ operation
                            fact = ReadFact(
                                source_file=source_file,
                                line_number=line_number,
                                dataset_urn=path,
                                dataset_type="hdfs",
                                confidence=0.75,
                                extraction_method=ExtractionMethod.REGEX,
                                evidence=match.group(0)[:100],
                                has_placeholders=has_placeholders
                            )
                            facts.append(fact)
                        else:
                            # WRITE operation (copy_from_local, move_from_local)
                            fact = WriteFact(
                                source_file=source_file,
                                line_number=line_number,
                                dataset_urn=path,
                                dataset_type="hdfs",
                                confidence=0.75,
                                extraction_method=ExtractionMethod.REGEX,
                                evidence=match.group(0)[:100],
                                has_placeholders=has_placeholders
                            )
                            facts.append(fact)
        
        # Extract JDBC operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name in ["jdbc_read", "jdbc_write"]:
                for match in pattern.finditer(content_joined):
                    table = match.group(1)
                    line_number = content[:match.start()].count("\n") + 1
                    has_placeholders = "$" in table or "{" in table
                    
                    urn = f"jdbc:///{table}"
                    dataset_type = "jdbc"
                    
                    if pattern_name == "jdbc_read":
                        fact = ReadFact(
                            source_file=source_file,
                            line_number=line_number,
                            dataset_urn=urn,
                            dataset_type=dataset_type,
                            confidence=0.80,
                            extraction_method=ExtractionMethod.REGEX,
                            evidence=match.group(0)[:100],
                            has_placeholders=has_placeholders
                        )
                        fact.params["dbtable"] = table
                        facts.append(fact)
                    else:
                        fact = WriteFact(
                            source_file=source_file,
                            line_number=line_number,
                            dataset_urn=urn,
                            dataset_type=dataset_type,
                            confidence=0.80,
                            extraction_method=ExtractionMethod.REGEX,
                            evidence=match.group(0)[:100],
                            has_placeholders=has_placeholders
                        )
                        fact.params["dbtable"] = table
                        facts.append(fact)
        
        # Use rule engine for additional patterns
        if self.rule_engine:
            matches = self.rule_engine.apply_rules(content_joined, "java")
            for match in matches:
                fact = self._match_to_fact(match, source_file)
                if fact:
                    facts.append(fact)
        
        return facts
    
    def _extract_variable_definitions(self, content: str, source_file: str) -> List[Fact]:
        """Extract variable definitions from Java code."""
        facts = []
        
        # Pattern: String varName = "value"; or static final String varName = "value";
        var_pattern = re.compile(
            r'(?:private\s+|public\s+|protected\s+)?(?:static\s+)?(?:final\s+)?String\s+(\w+)\s*=\s*"([^"]+)"\s*;',
            re.MULTILINE
        )
        
        for match in var_pattern.finditer(content):
            var_name = match.group(1)
            value = match.group(2).strip()
            line_number = content[:match.start()].count("\n") + 1
            
            fact = ConfigFact(
                source_file=source_file,
                line_number=line_number,
                config_key=var_name,
                config_value=value,
                confidence=0.90,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0).strip()
            )
            facts.append(fact)
        
        return facts
    
    def _extract_hdfs_operations(self, content: str, source_file: str, var_map: dict) -> List[Fact]:
        """Extract HDFS FileSystem operations with variable resolution."""
        facts = []
        
        # Pattern: new Path(CONSTANT + "/" + variable)
        path_concat_pattern = re.compile(
            r'new Path\((\w+)\s*\+\s*"([^"]*)"\s*\+\s*(\w+)\)',
            re.MULTILINE
        )
        
        for match in path_concat_pattern.finditer(content):
            const_name = match.group(1)
            separator = match.group(2)
            var_name = match.group(3)
            
            # Resolve constant from var_map
            if const_name in var_map:
                base_path = var_map[const_name]
                # Create parametric path
                resolved_path = f"{base_path}{separator}{{{var_name}}}"
                
                # Determine if this is a read or write based on context
                # Look ahead for fs.exists, fs.delete, fs.mkdirs
                context_start = max(0, match.start() - 100)
                context_end = min(len(content), match.end() + 200)
                context = content[context_start:context_end]
                
                # Check for write operations
                is_write = any(op in context for op in ['mkdirs', 'create', 'copyFromLocalFile', 'moveFromLocalFile'])
                # Check for read operations
                is_read = any(op in context for op in ['exists', 'copyToLocalFile', 'open'])
                
                if is_write:
                    fact = WriteFact(
                        source_file=source_file,
                        line_number=1,
                        dataset_urn=resolved_path,
                        dataset_type="hdfs",
                        confidence=0.75,
                        extraction_method=ExtractionMethod.REGEX,
                        evidence=match.group(0),
                        has_placeholders=True
                    )
                    facts.append(fact)
                elif is_read:
                    fact = ReadFact(
                        source_file=source_file,
                        line_number=1,
                        dataset_urn=resolved_path,
                        dataset_type="hdfs",
                        confidence=0.75,
                        extraction_method=ExtractionMethod.REGEX,
                        evidence=match.group(0),
                        has_placeholders=True
                    )
                    facts.append(fact)
        
        return facts
    
    def _extract_sql_templates(self, content: str, source_file: str, var_map: dict) -> List[Fact]:
        """Extract SQL from String.format() calls with variable resolution."""
        facts = []
        
        # First, normalize string concatenations (handle multiline strings with +)
        # "string1" + "string2" -> "string1string2"
        content_normalized = content
        # Remove line breaks and + between strings
        content_normalized = re.sub(r'"\s*\+\s*\n\s*"', '', content_normalized)
        content_normalized = re.sub(r'"\s*\+\s*"', '', content_normalized)
        
        # Pattern: String.format("template", args...)
        format_pattern = re.compile(
            r'String\.format\(\s*"([^"]+)"\s*,\s*([^)]+)\)',
            re.DOTALL
        )
        
        for match in format_pattern.finditer(content_normalized):
            template = match.group(1)
            args_str = match.group(2)
            
            # Split arguments - handle complex expressions
            args = []
            paren_depth = 0
            current_arg = ""
            for char in args_str:
                if char == '(' :
                    paren_depth += 1
                    current_arg += char
                elif char == ')':
                    paren_depth -= 1
                    current_arg += char
                elif char == ',' and paren_depth == 0:
                    args.append(current_arg.strip())
                    current_arg = ""
                else:
                    current_arg += char
            if current_arg.strip():
                args.append(current_arg.strip())
            
            # Replace %s placeholders with resolved values or {varname}
            resolved_sql = template
            placeholder_idx = 0
            
            for arg in args:
                if '%s' in resolved_sql:
                    # Extract simple variable name (handle .toString(), .replace(), etc.)
                    var_name = arg.split('.')[0].split('(')[0].strip()
                    
                    # Try to resolve from var_map
                    if var_name in var_map:
                        resolved_value = var_map[var_name]
                        resolved_sql = resolved_sql.replace('%s', resolved_value, 1)
                    else:
                        # Use placeholder
                        resolved_sql = resolved_sql.replace('%s', f'{{{var_name}}}', 1)
                    placeholder_idx += 1
            
            # Extract table names from SQL
            # Pattern: CREATE [EXTERNAL] TABLE schema.table
            create_table_pattern = re.compile(r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_{}]+\.[a-zA-Z0-9_{}]+)', re.IGNORECASE)
            for table_match in create_table_pattern.finditer(resolved_sql):
                table_name = table_match.group(1)
                fact = WriteFact(
                    source_file=source_file,
                    line_number=1,
                    dataset_urn=f"hive:///{table_name}",
                    dataset_type="hive",
                    confidence=0.80,
                    extraction_method=ExtractionMethod.REGEX,
                    evidence=f"String.format CREATE TABLE {table_name}",
                    has_placeholders='{' in table_name
                )
                facts.append(fact)
            
            # Pattern: INSERT OVERWRITE DIRECTORY 'path'
            insert_dir_pattern = re.compile(r"INSERT\s+OVERWRITE\s+DIRECTORY\s+'([^']+)'", re.IGNORECASE)
            for insert_match in insert_dir_pattern.finditer(resolved_sql):
                path = insert_match.group(1)
                fact = WriteFact(
                    source_file=source_file,
                    line_number=1,
                    dataset_urn=path,
                    dataset_type="hdfs",
                    confidence=0.80,
                    extraction_method=ExtractionMethod.REGEX,
                    evidence=f"INSERT OVERWRITE DIRECTORY",
                    has_placeholders='{' in path
                )
                facts.append(fact)
            
            # Pattern: FROM table
            from_pattern = re.compile(r'FROM\s+([a-zA-Z0-9_{}]+\.[a-zA-Z0-9_{}]+)', re.IGNORECASE)
            for from_match in from_pattern.finditer(resolved_sql):
                table_name = from_match.group(1)
                fact = ReadFact(
                    source_file=source_file,
                    line_number=1,
                    dataset_urn=f"hive:///{table_name}",
                    dataset_type="hive",
                    confidence=0.80,
                    extraction_method=ExtractionMethod.REGEX,
                    evidence=f"FROM {table_name}",
                    has_placeholders='{' in table_name
                )
                facts.append(fact)
            
            # Pattern: LOCATION 'path'
            location_pattern = re.compile(r"LOCATION\s+'([^']+)'", re.IGNORECASE)
            for loc_match in location_pattern.finditer(resolved_sql):
                path = loc_match.group(1)
                # LOCATION in CREATE TABLE is typically a read reference
                fact = ReadFact(
                    source_file=source_file,
                    line_number=1,
                    dataset_urn=path,
                    dataset_type="hdfs",
                    confidence=0.75,
                    extraction_method=ExtractionMethod.REGEX,
                    evidence=f"LOCATION",
                    has_placeholders='{' in path
                )
                facts.append(fact)
        
        return facts
    
    def _remove_comments(self, content: str) -> str:
        """Remove Java comments (// and /* */) while preserving strings."""
        result = []
        i = 0
        in_string = False
        string_char = None
        
        while i < len(content):
            # Check for escape sequences
            if in_string and content[i] == '\\' and i + 1 < len(content):
                result.append(content[i:i+2])
                i += 2
                continue
            
            # Toggle string state
            if content[i] in ('"', "'") and (i == 0 or content[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = content[i]
                elif content[i] == string_char:
                    in_string = False
                    string_char = None
                result.append(content[i])
                i += 1
                continue
            
            # If we're in a string, just copy the character
            if in_string:
                result.append(content[i])
                i += 1
                continue
            
            # Check for single-line comment
            if i + 1 < len(content) and content[i:i+2] == '//':
                # Skip until end of line
                while i < len(content) and content[i] != '\n':
                    i += 1
                continue
            
            # Check for multi-line comment
            if i + 1 < len(content) and content[i:i+2] == '/*':
                # Skip until end of comment
                i += 2
                while i + 1 < len(content):
                    if content[i:i+2] == '*/':
                        i += 2
                        break
                    i += 1
                continue
            
            # Regular character
            result.append(content[i])
            i += 1
        
        return ''.join(result)
    
    def _match_to_fact(self, match: dict, source_file: str) -> Optional[Fact]:
        """Convert rule match to fact."""
        action = match.get("action", "").upper()
        
        if action == "READ_HDFS_PATH":
            return ReadFact(
                source_file=source_file,
                line_number=match.get("line_number", 0),
                dataset_urn=match.get("path", ""),
                dataset_type="hdfs",
                confidence=match.get("confidence", 0.5),
                extraction_method=ExtractionMethod.RULE,
                evidence=match.get("matched_text", "")[:100],
                has_placeholders="${" in match.get("path", "")
            )
        elif action == "WRITE_HDFS_PATH":
            return WriteFact(
                source_file=source_file,
                line_number=match.get("line_number", 0),
                dataset_urn=match.get("path", ""),
                dataset_type="hdfs",
                confidence=match.get("confidence", 0.5),
                extraction_method=ExtractionMethod.RULE,
                evidence=match.get("matched_text", "")[:100],
                has_placeholders="${" in match.get("path", "")
            )
        elif action == "READ_HIVE_TABLE":
            return ReadFact(
                source_file=source_file,
                line_number=match.get("line_number", 0),
                dataset_urn=f"hive:///{match.get('table', '')}",
                dataset_type="hive",
                confidence=match.get("confidence", 0.5),
                extraction_method=ExtractionMethod.RULE,
                evidence=match.get("matched_text", "")[:100],
                has_placeholders=False
            )
        elif action == "WRITE_HIVE_TABLE":
            return WriteFact(
                source_file=source_file,
                line_number=match.get("line_number", 0),
                dataset_urn=f"hive:///{match.get('table', '')}",
                dataset_type="hive",
                confidence=match.get("confidence", 0.5),
                extraction_method=ExtractionMethod.RULE,
                evidence=match.get("matched_text", "")[:100],
                has_placeholders=False
            )
        
        return None

