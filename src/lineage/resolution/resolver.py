"""Variable resolver for path and config resolution."""

from typing import Optional, Set
import re

from .symbol_table import SymbolTable
from .canonicalizer import PathCanonicalizer


class VariableResolver:
    """Resolves variables and templates in paths and configs."""
    
    def __init__(
        self,
        symbol_table: SymbolTable,
        canonicalizer: Optional[PathCanonicalizer] = None
    ) -> None:
        self.symbol_table = symbol_table
        self.canonicalizer = canonicalizer or PathCanonicalizer()
        
        # Patterns for variable references
        self.patterns = [
            re.compile(r'\$\{([^}]+)\}'),  # ${var}
            re.compile(r'\$([A-Z_][A-Z0-9_]*)'),  # $VAR
        ]
    
    def resolve(self, text: str, max_iterations: int = 10) -> tuple[str, bool]:
        """Resolve variables in text.
        
        Returns:
            Tuple of (resolved_text, fully_resolved)
        """
        if not text:
            return text, True
        
        resolved = text
        iteration = 0
        
        while iteration < max_iterations:
            prev = resolved
            resolved = self._resolve_iteration(resolved)
            iteration += 1
            
            if resolved == prev:
                # No more changes
                break
        
        # Check if fully resolved
        fully_resolved = not self._has_variables(resolved)
        
        return resolved, fully_resolved
    
    def _resolve_iteration(self, text: str) -> str:
        """Perform one iteration of variable resolution."""
        result = text
        
        for pattern in self.patterns:
            def replace_fn(match: re.Match) -> str:
                var_name = match.group(1)
                value = self.symbol_table.resolve(var_name)
                if value is not None:
                    return value
                return match.group(0)  # Keep original if not resolved
            
            result = pattern.sub(replace_fn, result)
        
        return result
    
    def _has_variables(self, text: str) -> bool:
        """Check if text contains unresolved variables."""
        for pattern in self.patterns:
            if pattern.search(text):
                return True
        return False
    
    def resolve_path(self, path: str) -> tuple[str, bool]:
        """Resolve and canonicalize a path.
        
        Returns:
            Tuple of (resolved_path, fully_resolved)
        """
        # Resolve variables
        resolved, fully_resolved = self.resolve(path)
        
        # Canonicalize
        resolved = self.canonicalizer.canonicalize(resolved)
        
        return resolved, fully_resolved
    
    def resolve_table(self, table: str) -> tuple[str, bool]:
        """Resolve and normalize a table name.
        
        Returns:
            Tuple of (resolved_table, fully_resolved)
        """
        # Resolve variables
        resolved, fully_resolved = self.resolve(table)
        
        # Normalize
        resolved = self.canonicalizer.normalize_hive_table(resolved)
        
        return resolved, fully_resolved
    
    def extract_variables(self, text: str) -> Set[str]:
        """Extract all variable names from text."""
        variables = set()
        
        for pattern in self.patterns:
            for match in pattern.finditer(text):
                var_name = match.group(1)
                variables.add(var_name)
        
        return variables

