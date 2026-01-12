"""Multi-format config file parser."""

from pathlib import Path
from typing import List, Optional, Dict, Any
import json
import yaml
import re
import configparser
import xml.etree.ElementTree as ET

from .base import BaseExtractor
from lineage.ir import Fact, ConfigFact, ExtractionMethod


class ConfigExtractor(BaseExtractor):
    """Extractor for configuration files."""
    
    def __init__(self) -> None:
        super().__init__()
    
    def extract(self, file_path: Path) -> List[Fact]:
        """Extract configuration facts from file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            return self.extract_from_content(content, str(file_path))
        
        except Exception as e:
            print(f"Error extracting from {file_path}: {e}")
            return []
    
    def extract_from_content(self, content: str, source_file: str) -> List[Fact]:
        """Extract facts from config content."""
        facts = []
        
        # Determine config format
        if source_file.endswith((".yaml", ".yml")):
            facts.extend(self._extract_yaml(content, source_file))
        elif source_file.endswith(".json"):
            facts.extend(self._extract_json(content, source_file))
        elif source_file.endswith(".properties"):
            facts.extend(self._extract_properties(content, source_file))
        elif source_file.endswith(".xml"):
            facts.extend(self._extract_xml(content, source_file))
        elif source_file.endswith(".conf"):
            # Try multiple formats
            try:
                facts.extend(self._extract_yaml(content, source_file))
            except:
                facts.extend(self._extract_properties(content, source_file))
        else:
            # Try to detect format
            facts.extend(self._extract_auto(content, source_file))
        
        return facts
    
    def _extract_yaml(self, content: str, source_file: str) -> List[Fact]:
        """Extract from YAML config."""
        facts = []
        
        try:
            data = yaml.safe_load(content)
            if isinstance(data, dict):
                facts.extend(self._extract_dict(data, source_file, "yaml"))
        except yaml.YAMLError as e:
            print(f"YAML parse error in {source_file}: {e}")
        
        return facts
    
    def _extract_json(self, content: str, source_file: str) -> List[Fact]:
        """Extract from JSON config."""
        facts = []
        
        try:
            data = json.loads(content)
            if isinstance(data, dict):
                facts.extend(self._extract_dict(data, source_file, "json"))
        except json.JSONDecodeError as e:
            print(f"JSON parse error in {source_file}: {e}")
        
        return facts
    
    def _extract_properties(self, content: str, source_file: str) -> List[Fact]:
        """Extract from .properties file."""
        facts = []
        
        # Simple key=value parsing
        for line_num, line in enumerate(content.split("\n"), 1):
            line = line.strip()
            
            # Skip comments and empty lines
            if not line or line.startswith("#") or line.startswith("!"):
                continue
            
            # Parse key=value
            if "=" in line:
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip()
                
                fact = ConfigFact(
                    source_file=source_file,
                    line_number=line_num,
                    confidence=0.90,
                    extraction_method=ExtractionMethod.REGEX,
                    evidence=line,
                    config_key=key,
                    config_value=value,
                    config_source="properties"
                )
                facts.append(fact)
        
        return facts
    
    def _extract_xml(self, content: str, source_file: str) -> List[Fact]:
        """Extract from XML config."""
        facts = []
        
        try:
            root = ET.fromstring(content)
            
            # Handle common XML config formats
            # Hadoop-style <property><name>...</name><value>...</value></property>
            for prop in root.findall(".//property"):
                name_elem = prop.find("name")
                value_elem = prop.find("value")
                
                if name_elem is not None and value_elem is not None:
                    key = name_elem.text
                    value = value_elem.text
                    
                    fact = ConfigFact(
                        source_file=source_file,
                        line_number=0,
                        confidence=0.90,
                        extraction_method=ExtractionMethod.JSON_PARSE,
                        evidence=f"<property>{key}={value}</property>",
                        config_key=key,
                        config_value=value,
                        config_source="xml"
                    )
                    facts.append(fact)
            
            # Generic XML attributes
            for elem in root.iter():
                for key, value in elem.attrib.items():
                    fact = ConfigFact(
                        source_file=source_file,
                        line_number=0,
                        confidence=0.70,
                        extraction_method=ExtractionMethod.JSON_PARSE,
                        evidence=f"{elem.tag}[@{key}={value}]",
                        config_key=f"{elem.tag}.{key}",
                        config_value=value,
                        config_source="xml"
                    )
                    facts.append(fact)
        
        except ET.ParseError as e:
            print(f"XML parse error in {source_file}: {e}")
        
        return facts
    
    def _extract_dict(
        self,
        data: Dict[str, Any],
        source_file: str,
        format: str,
        prefix: str = ""
    ) -> List[Fact]:
        """Recursively extract from dictionary."""
        facts = []
        
        for key, value in data.items():
            full_key = f"{prefix}.{key}" if prefix else key
            
            if isinstance(value, dict):
                # Recurse into nested dict
                facts.extend(self._extract_dict(value, source_file, format, full_key))
            elif isinstance(value, (str, int, float, bool)):
                # Create config fact
                fact = ConfigFact(
                    source_file=source_file,
                    line_number=0,
                    confidence=0.90,
                    extraction_method=ExtractionMethod.JSON_PARSE,
                    evidence=f"{full_key}={value}",
                    config_key=full_key,
                    config_value=str(value),
                    config_source=format
                )
                facts.append(fact)
            elif isinstance(value, list):
                # Handle lists
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        facts.extend(
                            self._extract_dict(item, source_file, format, f"{full_key}[{i}]")
                        )
                    else:
                        fact = ConfigFact(
                            source_file=source_file,
                            line_number=0,
                            confidence=0.80,
                            extraction_method=ExtractionMethod.JSON_PARSE,
                            evidence=f"{full_key}[{i}]={item}",
                            config_key=f"{full_key}[{i}]",
                            config_value=str(item),
                            config_source=format
                        )
                        facts.append(fact)
        
        return facts
    
    def _extract_auto(self, content: str, source_file: str) -> List[Fact]:
        """Auto-detect format and extract."""
        # Try JSON first
        try:
            return self._extract_json(content, source_file)
        except:
            pass
        
        # Try YAML
        try:
            return self._extract_yaml(content, source_file)
        except:
            pass
        
        # Fallback to properties
        return self._extract_properties(content, source_file)
    
    def get_confidence_base(self) -> float:
        """Get base confidence for config extraction."""
        return 0.85

