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
        
        # Scala-specific patterns
        self.patterns = {
            "read_parquet": re.compile(r'\.read\.parquet\("([^"]+)"\)'),
            "read_csv": re.compile(r'\.read\.csv\("([^"]+)"\)'),
            "read_json": re.compile(r'\.read\.json\("([^"]+)"\)'),
            "read_orc": re.compile(r'\.read\.orc\("([^"]+)"\)'),
            "read_table": re.compile(r'\.table\("([^"]+)"\)'),
            "write_parquet": re.compile(r'\.write\.parquet\("([^"]+)"\)'),
            "write_csv": re.compile(r'\.write\.csv\("([^"]+)"\)'),
            "write_json": re.compile(r'\.write\.json\("([^"]+)"\)'),
            "write_orc": re.compile(r'\.write\.orc\("([^"]+)"\)'),
            "save_as_table": re.compile(r'\.saveAsTable\("([^"]+)"\)'),
            "insert_into": re.compile(r'\.insertInto\("([^"]+)"\)'),
            "spark_sql": re.compile(r'\.sql\("([^"]+)"\)', re.DOTALL),
            # String interpolation patterns
            "interpolated_read": re.compile(r'\.read\.\w+\(s"([^"]+)"\)'),
            "interpolated_write": re.compile(r'\.write\.\w+\(s"([^"]+)"\)'),
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
        
        # Extract read operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("read_"):
                for match in pattern.finditer(content_no_comments):
                    path = match.group(1)
                    line_number = content[:match.start()].count("\n") + 1
                    
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
                        evidence=match.group(0),
                        has_placeholders="${" in path or "$" in path
                    )
                    facts.append(fact)
        
        # Extract write operations
        for pattern_name, pattern in self.patterns.items():
            if pattern_name.startswith("write_") or pattern_name in ["save_as_table", "insert_into"]:
                for match in pattern.finditer(content_no_comments):
                    path = match.group(1)
                    line_number = content[:match.start()].count("\n") + 1
                    
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
                        has_placeholders="${" in path or "$" in path
                    )
                    facts.append(fact)
        
        # Use rule engine for additional patterns
        if self.rule_engine:
            matches = self.rule_engine.apply_rules(content_no_comments, "scala")
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

