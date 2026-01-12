"""IR module initialization."""

from .fact import Fact, FactType, ExtractionMethod, ReadFact, WriteFact, ConfigFact, JobDependencyFact
from .store import FactStore

__all__ = [
    "Fact",
    "FactType",
    "ExtractionMethod",
    "ReadFact",
    "WriteFact",
    "ConfigFact",
    "JobDependencyFact",
    "FactStore"
]

