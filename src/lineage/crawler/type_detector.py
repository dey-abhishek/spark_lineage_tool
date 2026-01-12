"""File type detection for lineage tool."""

from pathlib import Path
from typing import Optional
from enum import Enum
import re


class FileType(Enum):
    """Supported file types for extraction."""
    
    PYSPARK = "pyspark"
    SCALA = "scala"
    HIVE = "hive"
    SHELL = "shell"
    NIFI = "nifi"
    PROPERTIES = "properties"
    YAML = "yaml"
    JSON = "json"
    XML = "xml"
    CONF = "conf"
    UNKNOWN = "unknown"


class TypeDetector:
    """Detects file types using multiple strategies."""
    
    # Extension-based detection
    EXTENSION_MAP = {
        ".py": FileType.PYSPARK,
        ".scala": FileType.SCALA,
        ".hql": FileType.HIVE,
        ".sql": FileType.HIVE,
        ".sh": FileType.SHELL,
        ".bash": FileType.SHELL,
        ".properties": FileType.PROPERTIES,
        ".yaml": FileType.YAML,
        ".yml": FileType.YAML,
        ".json": FileType.JSON,
        ".xml": FileType.XML,
        ".conf": FileType.CONF,
    }
    
    # Shebang detection patterns
    SHEBANG_PATTERNS = {
        r"#!/usr/bin/env python": FileType.PYSPARK,
        r"#!/usr/bin/python": FileType.PYSPARK,
        r"#!/bin/bash": FileType.SHELL,
        r"#!/bin/sh": FileType.SHELL,
        r"#!/usr/bin/env bash": FileType.SHELL,
    }
    
    # Content-based patterns (for files without extensions)
    CONTENT_PATTERNS = {
        FileType.PYSPARK: [
            r"from pyspark",
            r"import pyspark",
            r"SparkSession",
            r"SparkContext",
        ],
        FileType.SCALA: [
            r"import org\.apache\.spark",
            r"object \w+ extends",
            r"class \w+ extends",
        ],
        FileType.HIVE: [
            r"^\s*(?:INSERT|SELECT|CREATE|DROP|ALTER)\s+",
            r"^\s*SET\s+hive\.",
        ],
        FileType.SHELL: [
            r"hdfs dfs",
            r"hadoop distcp",
            r"spark-submit",
            r"hive -[ef]",
        ],
        FileType.NIFI: [
            r'"encodingVersion"',
            r'"processors"',
            r'"controllerServices"',
        ],
    }
    
    def __init__(self) -> None:
        """Initialize the type detector."""
        # Compile content patterns
        self._compiled_patterns = {
            file_type: [re.compile(pattern, re.IGNORECASE | re.MULTILINE) 
                       for pattern in patterns]
            for file_type, patterns in self.CONTENT_PATTERNS.items()
        }
    
    def detect_from_extension(self, file_path: Path) -> Optional[FileType]:
        """Detect file type from extension."""
        suffix = file_path.suffix.lower()
        return self.EXTENSION_MAP.get(suffix)
    
    def detect_from_shebang(self, file_path: Path) -> Optional[FileType]:
        """Detect file type from shebang line."""
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                first_line = f.readline().strip()
            
            if first_line.startswith("#!"):
                for pattern, file_type in self.SHEBANG_PATTERNS.items():
                    if re.match(pattern, first_line):
                        return file_type
        except (IOError, UnicodeDecodeError):
            pass
        
        return None
    
    def detect_from_content(
        self,
        file_path: Path,
        max_lines: int = 100
    ) -> Optional[FileType]:
        """Detect file type from content analysis."""
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                # Read first N lines for analysis
                content = "".join(f.readline() for _ in range(max_lines))
            
            # Score each file type based on pattern matches
            scores = {file_type: 0 for file_type in FileType}
            
            for file_type, patterns in self._compiled_patterns.items():
                for pattern in patterns:
                    if pattern.search(content):
                        scores[file_type] += 1
            
            # Return type with highest score (if > 0)
            max_score = max(scores.values())
            if max_score > 0:
                for file_type, score in scores.items():
                    if score == max_score:
                        return file_type
        
        except (IOError, UnicodeDecodeError):
            pass
        
        return None
    
    def detect_file_type(self, file_path: Path) -> FileType:
        """Detect file type using multiple strategies."""
        # Strategy 1: Check extension
        file_type = self.detect_from_extension(file_path)
        if file_type:
            return file_type
        
        # Strategy 2: Check shebang
        file_type = self.detect_from_shebang(file_path)
        if file_type:
            return file_type
        
        # Strategy 3: Content sniffing
        file_type = self.detect_from_content(file_path)
        if file_type:
            return file_type
        
        return FileType.UNKNOWN
    
    def is_pyspark_file(self, file_path: Path) -> bool:
        """Check if file is a PySpark file."""
        file_type = self.detect_file_type(file_path)
        return file_type == FileType.PYSPARK
    
    def is_extractable(self, file_path: Path) -> bool:
        """Check if file type can be extracted."""
        file_type = self.detect_file_type(file_path)
        return file_type != FileType.UNKNOWN

