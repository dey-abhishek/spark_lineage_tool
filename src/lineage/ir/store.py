"""IR fact storage and management."""

from typing import List, Dict, Optional, Set
from pathlib import Path
import json
from collections import defaultdict

from .fact import Fact, FactType


class FactStore:
    """Storage and management for extracted facts."""
    
    def __init__(self) -> None:
        self.facts: List[Fact] = []
        self._facts_by_file: Dict[str, List[Fact]] = defaultdict(list)
        self._facts_by_type: Dict[FactType, List[Fact]] = defaultdict(list)
        self._facts_by_dataset: Dict[str, List[Fact]] = defaultdict(list)
    
    def add_fact(self, fact: Fact) -> None:
        """Add a fact to the store."""
        self.facts.append(fact)
        self._facts_by_file[fact.source_file].append(fact)
        self._facts_by_type[fact.fact_type].append(fact)
        
        if fact.dataset_urn:
            self._facts_by_dataset[fact.dataset_urn].append(fact)
    
    def add_facts(self, facts: List[Fact]) -> None:
        """Add multiple facts to the store."""
        for fact in facts:
            self.add_fact(fact)
    
    def get_facts_by_file(self, file_path: str) -> List[Fact]:
        """Get all facts from a specific file."""
        return self._facts_by_file.get(file_path, [])
    
    def get_facts_by_type(self, fact_type: FactType) -> List[Fact]:
        """Get all facts of a specific type."""
        return self._facts_by_type.get(fact_type, [])
    
    def get_facts_by_dataset(self, dataset_urn: str) -> List[Fact]:
        """Get all facts related to a specific dataset."""
        return self._facts_by_dataset.get(dataset_urn, [])
    
    def get_read_facts(self) -> List[Fact]:
        """Get all read facts."""
        return self.get_facts_by_type(FactType.READ)
    
    def get_write_facts(self) -> List[Fact]:
        """Get all write facts."""
        return self.get_facts_by_type(FactType.WRITE)
    
    def get_unique_datasets(self) -> Set[str]:
        """Get set of all unique dataset URNs."""
        return set(self._facts_by_dataset.keys())
    
    def get_unique_files(self) -> Set[str]:
        """Get set of all unique source files."""
        return set(self._facts_by_file.keys())
    
    def filter_facts(
        self,
        fact_type: Optional[FactType] = None,
        min_confidence: Optional[float] = None,
        source_file: Optional[str] = None,
        has_placeholders: Optional[bool] = None
    ) -> List[Fact]:
        """Filter facts based on criteria."""
        result = self.facts
        
        if fact_type is not None:
            result = [f for f in result if f.fact_type == fact_type]
        
        if min_confidence is not None:
            result = [f for f in result if f.confidence >= min_confidence]
        
        if source_file is not None:
            result = [f for f in result if f.source_file == source_file]
        
        if has_placeholders is not None:
            result = [f for f in result if f.has_placeholders == has_placeholders]
        
        return result
    
    def to_json(self, output_path: Path) -> None:
        """Export facts to JSON file."""
        data = {
            "total_facts": len(self.facts),
            "facts": [fact.to_dict() for fact in self.facts]
        }
        
        with open(output_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    
    def from_json(self, input_path: Path) -> None:
        """Load facts from JSON file."""
        with open(input_path, "r") as f:
            data = json.load(f)
        
        self.facts = [Fact.from_dict(fact_dict) for fact_dict in data["facts"]]
        
        # Rebuild indexes
        self._facts_by_file.clear()
        self._facts_by_type.clear()
        self._facts_by_dataset.clear()
        
        for fact in self.facts:
            self._facts_by_file[fact.source_file].append(fact)
            self._facts_by_type[fact.fact_type].append(fact)
            if fact.dataset_urn:
                self._facts_by_dataset[fact.dataset_urn].append(fact)
    
    def get_stats(self) -> Dict[str, int]:
        """Get statistics about stored facts."""
        return {
            "total_facts": len(self.facts),
            "unique_files": len(self._facts_by_file),
            "unique_datasets": len(self._facts_by_dataset),
            "read_facts": len(self.get_read_facts()),
            "write_facts": len(self.get_write_facts()),
            "high_confidence": len([f for f in self.facts if f.confidence >= 0.8]),
            "medium_confidence": len([f for f in self.facts if 0.5 <= f.confidence < 0.8]),
            "low_confidence": len([f for f in self.facts if f.confidence < 0.5])
        }
    
    def clear(self) -> None:
        """Clear all facts from the store."""
        self.facts.clear()
        self._facts_by_file.clear()
        self._facts_by_type.clear()
        self._facts_by_dataset.clear()

