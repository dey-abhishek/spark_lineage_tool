"""Base extractor interface."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List

from lineage.ir import Fact


class BaseExtractor(ABC):
    """Abstract base class for all extractors."""
    
    def __init__(self) -> None:
        """Initialize the extractor."""
        self.facts: List[Fact] = []
    
    @abstractmethod
    def extract(self, file_path: Path) -> List[Fact]:
        """Extract facts from a file.
        
        Args:
            file_path: Path to the file to extract from
            
        Returns:
            List of extracted facts
        """
        pass
    
    def extract_from_content(self, content: str, source_file: str) -> List[Fact]:
        """Extract facts from file content (optional implementation).
        
        Args:
            content: File content as string
            source_file: Source file path for reference
            
        Returns:
            List of extracted facts
        """
        raise NotImplementedError("Subclass must implement extract_from_content")
    
    def can_extract(self, file_path: Path) -> bool:
        """Check if this extractor can handle the file.
        
        Args:
            file_path: Path to check
            
        Returns:
            True if extractor can handle this file
        """
        return True
    
    def get_confidence_base(self) -> float:
        """Get base confidence score for this extractor type.
        
        Returns:
            Base confidence score (0.0 to 1.0)
        """
        return 0.5

