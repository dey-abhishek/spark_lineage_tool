"""Path canonicalization utilities."""

from pathlib import Path
from typing import Optional
import re
from urllib.parse import urlparse


class PathCanonicalizer:
    """Canonicalizes and normalizes paths."""
    
    def __init__(self, base_dirs: Optional[dict] = None) -> None:
        self.base_dirs = base_dirs or {}
    
    def canonicalize(self, path: str) -> str:
        """Canonicalize a path."""
        if not path:
            return path
        
        # Remove duplicate slashes
        path = re.sub(r'/+', '/', path)
        
        # Remove trailing slash (unless root)
        if len(path) > 1 and path.endswith('/'):
            path = path.rstrip('/')
        
        # Resolve .. and .
        path = self._resolve_relative_parts(path)
        
        # Add protocol if missing and looks like HDFS path
        if path.startswith('/') and not path.startswith(('hdfs://', 'file://', 's3://', 'wasb://')):
            # Check if it's an absolute local path or HDFS path
            if self._looks_like_hdfs_path(path):
                path = f"hdfs://{path}"
        
        return path
    
    def _resolve_relative_parts(self, path: str) -> str:
        """Resolve .. and . in path."""
        # Parse URL if present
        parsed = urlparse(path)
        
        if parsed.scheme:
            # Has scheme (hdfs://, etc.)
            parts = parsed.path.split('/')
        else:
            parts = path.split('/')
        
        resolved = []
        for part in parts:
            if part == '..':
                if resolved and resolved[-1] != '..':
                    resolved.pop()
            elif part and part != '.':
                resolved.append(part)
        
        resolved_path = '/'.join(resolved)
        
        if parsed.scheme:
            return f"{parsed.scheme}://{parsed.netloc}/{resolved_path}"
        elif path.startswith('/'):
            return '/' + resolved_path
        else:
            return resolved_path
    
    def _looks_like_hdfs_path(self, path: str) -> bool:
        """Heuristic to detect HDFS paths."""
        hdfs_indicators = [
            '/data/',
            '/user/',
            '/warehouse/',
            '/tmp/',
            '/prod/',
            '/staging/'
        ]
        return any(indicator in path for indicator in hdfs_indicators)
    
    def resolve_base_dir(self, path: str, base_key: str) -> str:
        """Resolve path relative to a base directory."""
        if base_key in self.base_dirs:
            base = self.base_dirs[base_key]
            if not path.startswith('/') and not '://' in path:
                # Relative path
                path = f"{base}/{path}"
        
        return self.canonicalize(path)
    
    def normalize_hive_table(self, table: str) -> str:
        """Normalize Hive table name."""
        table = table.strip('`"\' ')
        
        # Add default database if not specified
        if '.' not in table:
            table = f"default.{table}"
        
        return table.lower()
    
    def is_temp_path(self, path: str, temp_patterns: Optional[list] = None) -> bool:
        """Check if path is a temporary/staging path."""
        if temp_patterns is None:
            temp_patterns = [
                '/tmp/',
                '/_temporary/',
                '/_SUCCESS',
                '/_logs/',
                '/_metadata/',
                '/checkpoints/',
                '/.spark',
                '/staging/temp'
            ]
        
        return any(pattern in path for pattern in temp_patterns)

