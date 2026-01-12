"""Rule engine for pattern-based extraction."""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Pattern
import re
import yaml
from pathlib import Path
from enum import Enum


class RuleAction(Enum):
    """Actions that rules can identify."""
    
    READ_HDFS_PATH = "READ_HDFS_PATH"
    WRITE_HDFS_PATH = "WRITE_HDFS_PATH"
    READ_HIVE_TABLE = "READ_HIVE_TABLE"
    WRITE_HIVE_TABLE = "WRITE_HIVE_TABLE"
    READ_LOCAL_PATH = "READ_LOCAL_PATH"
    WRITE_LOCAL_PATH = "WRITE_LOCAL_PATH"
    CONFIG_REFERENCE = "CONFIG_REFERENCE"
    JOB_INVOCATION = "JOB_INVOCATION"


@dataclass
class ExtractionRule:
    """A single extraction rule."""
    
    rule_id: str
    applies_to: List[str]  # File types: pyspark, scala, hive, shell, nifi
    pattern: str
    action: RuleAction
    confidence: float
    description: str = ""
    multiline: bool = False
    case_sensitive: bool = True
    capture_groups: Dict[str, str] = None  # Named capture groups
    
    _compiled_pattern: Optional[Pattern] = None
    
    def __post_init__(self) -> None:
        """Compile the regex pattern."""
        if self.capture_groups is None:
            self.capture_groups = {}
        
        flags = 0 if self.case_sensitive else re.IGNORECASE
        if self.multiline:
            flags |= re.MULTILINE | re.DOTALL
        
        self._compiled_pattern = re.compile(self.pattern, flags)
    
    def match(self, text: str) -> List[Dict[str, Any]]:
        """Match the rule against text and return extracted data."""
        matches = []
        
        for match in self._compiled_pattern.finditer(text):
            result = {
                "rule_id": self.rule_id,
                "action": self.action,
                "confidence": self.confidence,
                "match_text": match.group(0),
                "start": match.start(),
                "end": match.end(),
                "groups": match.groupdict()
            }
            matches.append(result)
        
        return matches
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExtractionRule":
        """Create rule from dictionary."""
        data = data.copy()
        data["action"] = RuleAction[data["action"]]
        return cls(**data)


class RuleEngine:
    """Engine for managing and executing extraction rules."""
    
    def __init__(self) -> None:
        self.rules: List[ExtractionRule] = []
        self._rules_by_type: Dict[str, List[ExtractionRule]] = {}
    
    def add_rule(self, rule: ExtractionRule) -> None:
        """Add a rule to the engine."""
        self.rules.append(rule)
        
        for file_type in rule.applies_to:
            if file_type not in self._rules_by_type:
                self._rules_by_type[file_type] = []
            self._rules_by_type[file_type].append(rule)
    
    def load_rules_from_yaml(self, yaml_path: Path) -> None:
        """Load rules from a YAML file."""
        with open(yaml_path, "r") as f:
            data = yaml.safe_load(f)
        
        rules_data = data.get("rules", [])
        for rule_dict in rules_data:
            rule = ExtractionRule.from_dict(rule_dict)
            self.add_rule(rule)
    
    def load_default_rules(self) -> None:
        """Load default built-in rules."""
        default_rules_path = Path(__file__).parent / "default_rules.yaml"
        if default_rules_path.exists():
            self.load_rules_from_yaml(default_rules_path)
    
    def get_rules_for_type(self, file_type: str) -> List[ExtractionRule]:
        """Get all rules applicable to a file type."""
        return self._rules_by_type.get(file_type, [])
    
    def apply_rules(
        self,
        text: str,
        file_type: str,
        min_confidence: float = 0.0
    ) -> List[Dict[str, Any]]:
        """Apply all applicable rules to text."""
        all_matches = []
        
        rules = self.get_rules_for_type(file_type)
        for rule in rules:
            if rule.confidence >= min_confidence:
                matches = rule.match(text)
                all_matches.extend(matches)
        
        # Sort by position in text
        all_matches.sort(key=lambda m: m["start"])
        
        return all_matches
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about loaded rules."""
        return {
            "total_rules": len(self.rules),
            "by_type": {
                file_type: len(rules)
                for file_type, rules in self._rules_by_type.items()
            }
        }


def create_default_engine() -> RuleEngine:
    """Create a rule engine with default rules loaded."""
    engine = RuleEngine()
    engine.load_default_rules()
    return engine

