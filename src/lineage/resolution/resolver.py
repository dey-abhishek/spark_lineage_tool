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
        
        # First, handle shell parameter expansion with defaults: ${var:-default}
        result = self._resolve_parameter_defaults(result)
        
        for pattern in self.patterns:
            def replace_fn(match: re.Match) -> str:
                var_name = match.group(1)
                value = self.symbol_table.resolve(var_name)
                if value is not None:
                    return value
                return match.group(0)  # Keep original if not resolved
            
            result = pattern.sub(replace_fn, result)
        
        return result
    
    def _resolve_parameter_defaults(self, text: str) -> str:
        """Resolve shell parameter expansion with defaults: ${var:-default}
        
        Extracts and uses the default value when parameter can't be resolved.
        Examples:
            ${1:-2024-01-15} -> 2024-01-15
            ${ENV:-prod} -> prod (if ENV not defined)
            ${2:-default} -> default
        """
        # Pattern: ${parameter:-default}
        param_default_pattern = re.compile(r'\$\{([^}:]+):-([^}]+)\}')
        
        def replace_with_default(match: re.Match) -> str:
            param_name = match.group(1)
            default_value = match.group(2)
            
            # Try to resolve the parameter from symbol table
            value = self.symbol_table.resolve(param_name)
            if value is not None:
                return value
            
            # Parameter not in symbol table, use default value
            # This handles positional parameters ($1, $2, etc.) and undefined env vars
            return default_value
        
        return param_default_pattern.sub(replace_with_default, text)
    
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

