"""Symbol table for variable tracking."""

from typing import Dict, Optional, Any
from dataclasses import dataclass, field


@dataclass
class Symbol:
    """A symbol (variable/parameter) in the codebase."""
    
    name: str
    value: Optional[str] = None
    source: str = "unknown"  # cli, env, properties, declared
    file: Optional[str] = None
    line: Optional[int] = None
    resolved: bool = False


class SymbolTable:
    """Manages symbols from various sources."""
    
    def __init__(self) -> None:
        self.symbols: Dict[str, Symbol] = {}
        self.env_vars: Dict[str, str] = {}
        self.cli_args: Dict[str, str] = {}
        self.properties: Dict[str, str] = {}
    
    def add_symbol(self, symbol: Symbol) -> None:
        """Add a symbol to the table."""
        self.symbols[symbol.name] = symbol
    
    def add_env_var(self, name: str, value: str) -> None:
        """Add an environment variable."""
        self.env_vars[name] = value
        self.add_symbol(Symbol(name=name, value=value, source="env", resolved=True))
    
    def add_cli_arg(self, name: str, value: str) -> None:
        """Add a CLI argument."""
        self.cli_args[name] = value
        self.add_symbol(Symbol(name=name, value=value, source="cli", resolved=True))
    
    def add_property(self, name: str, value: str) -> None:
        """Add a property."""
        self.properties[name] = value
        self.add_symbol(Symbol(name=name, value=value, source="properties", resolved=True))
    
    def get_symbol(self, name: str) -> Optional[Symbol]:
        """Get a symbol by name."""
        return self.symbols.get(name)
    
    def resolve(self, name: str) -> Optional[str]:
        """Resolve a symbol name to its value."""
        # Try direct lookup
        if name in self.symbols and self.symbols[name].value:
            return self.symbols[name].value
        
        # Try environment variables
        if name in self.env_vars:
            return self.env_vars[name]
        
        # Try CLI args
        if name in self.cli_args:
            return self.cli_args[name]
        
        # Try properties
        if name in self.properties:
            return self.properties[name]
        
        return None
    
    def merge(self, other: "SymbolTable") -> None:
        """Merge another symbol table into this one."""
        self.symbols.update(other.symbols)
        self.env_vars.update(other.env_vars)
        self.cli_args.update(other.cli_args)
        self.properties.update(other.properties)

