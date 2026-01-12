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
            re.compile(r'\$\{([^}]+)\}'),  # ${var} - braced variables (shell, Scala)
            re.compile(r'(?<!\$)\{([^}]+)\}'),  # {var} - braced without dollar (Python f-strings), but not ${var}
            re.compile(r'\$([A-Z_][A-Z0-9_]*)'),  # $VAR - uppercase environment-style
            re.compile(r'\$([a-z][a-zA-Z0-9_]*)'),  # $var - lowercase/camelCase (Scala style)
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
        
        # Handle Hive-specific variable syntax: ${hiveconf:var}, ${hivevar:var}
        result = self._resolve_hive_variables(result)
        
        # Common defaults for non-prefixed variables (used by NiFi, shell, etc.)
        common_var_defaults = {
            'processing_date': '2026-01-13',
            'run_date': '2026-01-13',
            'env': 'prod',
            # Function parameter defaults (sample values for reporting)
            'remote_file': 'data_file.csv',
            'local_path': '/data/incoming',
            'file': 'data_file',
            'input_path': '/data/input',
            'output_path': '/data/output',
            # Date partitioning variables (used in NiFi, S3, HDFS paths)
            'year': '2026',
            'month': '01',
            'day': '13',
            # Config/environment variables
            'config.environment': 'prod',
            'config.runDate': '2026-01-13',
            '...': 'prod',  # Truncated/incomplete extractions
        }
        
        for pattern in self.patterns:
            def replace_fn(match: re.Match) -> str:
                var_name = match.group(1)
                value = self.symbol_table.resolve(var_name)
                if value is not None:
                    return value
                # Try common defaults
                if var_name in common_var_defaults:
                    return common_var_defaults[var_name]
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
    
    def _resolve_hive_variables(self, text: str) -> str:
        """Resolve Hive-specific variable syntax: ${hiveconf:var}, ${hivevar:var}
        
        Hive allows these formats:
            ${hiveconf:var_name} - Configuration variables
            ${hivevar:var_name}  - User-defined variables
            ${system:var_name}   - System properties
            ${env:var_name}      - Environment variables
        
        We strip the prefix and try to resolve the variable name.
        """
        # Pattern: ${hiveconf:var}, ${hivevar:var}, etc.
        hive_var_pattern = re.compile(r'\$\{(hiveconf|hivevar|system|env):([^}]+)\}', re.IGNORECASE)
        
        # Common Hive parameter defaults (for typical naming conventions)
        common_defaults = {
            'analytics_db': 'prod_analytics',
            'reports_db': 'prod_reports',
            'warehouse_db': 'prod_warehouse',
            'staging_db': 'prod_staging',
            'env': 'prod',
            # Common table references
            'source_products': 'catalog_db.products',
            'source_orders': 'prod_db.orders',
            'source_customers': 'prod_db.customers',
            'target_table': 'analytics_db.target_table',
            # Common date/processing parameters
            'processing_date': '2026-01-13',
            'run_date': '2026-01-13',
        }
        
        def resolve_hive_var(match: re.Match) -> str:
            prefix = match.group(1).lower()
            var_name = match.group(2)
            
            # Try to resolve the variable name directly
            value = self.symbol_table.resolve(var_name)
            if value is not None:
                return value
            
            # Try with the prefix (some configs might store it as "HIVECONF:var")
            prefixed_name = f"{prefix.upper()}:{var_name}"
            value = self.symbol_table.resolve(prefixed_name)
            if value is not None:
                return value
            
            # Try common defaults for typical Hive parameters
            if var_name.lower() in common_defaults:
                return common_defaults[var_name.lower()]
            
            # Keep original if not resolved
            return match.group(0)
        
        return hive_var_pattern.sub(resolve_hive_var, text)
    
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

