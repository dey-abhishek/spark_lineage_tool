"""Intermediate Representation (IR) fact models."""

from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from enum import Enum
from datetime import datetime
import uuid


class FactType(Enum):
    """Types of facts that can be extracted."""
    
    READ = "READ"
    WRITE = "WRITE"
    CONFIG = "CONFIG"
    JOB_DEPENDENCY = "JOB_DEPENDENCY"
    MODULE_REFERENCE = "MODULE_REFERENCE"
    TABLE_LOCATION = "TABLE_LOCATION"


class ExtractionMethod(Enum):
    """Method used to extract the fact."""
    
    AST = "AST"
    SQL_PARSE = "SQL_PARSE"
    REGEX = "REGEX"
    JSON_PARSE = "JSON_PARSE"
    HEURISTIC = "HEURISTIC"


@dataclass
class Fact:
    """A single extracted fact about data lineage."""
    
    fact_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    fact_type: FactType = FactType.READ
    source_file: str = ""
    line_number: int = 0
    confidence: float = 0.5
    
    # Dataset identification
    dataset_urn: Optional[str] = None
    dataset_type: Optional[str] = None  # hdfs, hive, local, etc.
    
    # Job identification
    job_name: Optional[str] = None
    module_name: Optional[str] = None
    
    # Metadata
    params: Dict[str, Any] = field(default_factory=dict)
    evidence: str = ""  # Code snippet that matched
    extraction_method: ExtractionMethod = ExtractionMethod.REGEX
    
    # Additional context
    has_placeholders: bool = False
    is_ambiguous: bool = False
    resolved_path: Optional[str] = None
    
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert fact to dictionary."""
        return {
            "fact_id": self.fact_id,
            "fact_type": self.fact_type.value,
            "source_file": self.source_file,
            "line_number": self.line_number,
            "confidence": self.confidence,
            "dataset_urn": self.dataset_urn,
            "dataset_type": self.dataset_type,
            "job_name": self.job_name,
            "module_name": self.module_name,
            "params": self.params,
            "evidence": self.evidence,
            "extraction_method": self.extraction_method.value,
            "has_placeholders": self.has_placeholders,
            "is_ambiguous": self.is_ambiguous,
            "resolved_path": self.resolved_path,
            "created_at": self.created_at.isoformat()
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Fact":
        """Create fact from dictionary."""
        data = data.copy()
        data["fact_type"] = FactType(data["fact_type"])
        data["extraction_method"] = ExtractionMethod(data["extraction_method"])
        if "created_at" in data and isinstance(data["created_at"], str):
            data["created_at"] = datetime.fromisoformat(data["created_at"])
        return cls(**data)


@dataclass
class ReadFact(Fact):
    """Fact representing a data read operation."""
    
    def __post_init__(self) -> None:
        self.fact_type = FactType.READ


@dataclass
class WriteFact(Fact):
    """Fact representing a data write operation."""
    
    def __post_init__(self) -> None:
        self.fact_type = FactType.WRITE
    
    write_mode: Optional[str] = None  # overwrite, append, etc.


@dataclass
class ConfigFact(Fact):
    """Fact representing a configuration value."""
    
    def __post_init__(self) -> None:
        self.fact_type = FactType.CONFIG
    
    config_key: Optional[str] = None
    config_value: Optional[str] = None
    config_source: Optional[str] = None


@dataclass
class JobDependencyFact(Fact):
    """Fact representing a job-to-job dependency."""
    
    def __post_init__(self) -> None:
        self.fact_type = FactType.JOB_DEPENDENCY
    
    dependency_job: Optional[str] = None
    dependency_type: Optional[str] = None  # spark-submit, shell call, etc.

