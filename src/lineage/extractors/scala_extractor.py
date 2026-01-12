"""Scala Spark extractor using pattern matching."""

from pathlib import Path
from typing import List, Optional
import re

from .base import BaseExtractor
from lineage.ir import Fact, ReadFact, WriteFact, ExtractionMethod
from lineage.rules import RuleEngine


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
            "read_format_load": re.compile(r'\.read\.format\("([^"]+)"\)\.load\((?:s?)"([^"]+)"\)', re.DOTALL),
            "write_parquet": re.compile(r'\.write(?:\.\w+\([^)]*\))*\.parquet\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "write_csv": re.compile(r'\.write(?:\.\w+\([^)]*\))*\.csv\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "write_json": re.compile(r'\.write(?:\.\w+\([^)]*\))*\.json\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "write_orc": re.compile(r'\.write(?:\.\w+\([^)]*\))*\.orc\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "save_as_table": re.compile(r'\.saveAsTable\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "insert_into": re.compile(r'\.insertInto\((?:(?:s?)"([^"]+)"|(\w+))\)', re.DOTALL),
            "spark_sql": re.compile(r'\.sql\((?:s?)"([^"]+)"\)', re.DOTALL),
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
        
        # Join lines for method chains (normalize whitespace around dots and newlines)
        content_joined = re.sub(r'\s*\n\s*\.', '.', content_no_comments)
        # Also normalize spaces around dots
        content_joined = re.sub(r'\s*\.\s*', '.', content_joined)
        
        # Extract read operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("read_"):
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
                    
                    fact = ReadFact(
                        source_file=source_file,
                        line_number=line_number,
                        dataset_urn=path,
                        dataset_type="delta" if format_type == "delta" else "hdfs",
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
        
        # Use rule engine for additional patterns
        if self.rule_engine:
            matches = self.rule_engine.apply_rules(content_joined, "scala")
            for match in matches:
                fact = self._match_to_fact(match, source_file)
                if fact:
                    facts.append(fact)
        
        return facts
    
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

