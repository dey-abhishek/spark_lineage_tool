"""Resolution module initialization."""

from .symbol_table import SymbolTable, Symbol
from .canonicalizer import PathCanonicalizer
from .resolver import VariableResolver

__all__ = ["SymbolTable", "Symbol", "PathCanonicalizer", "VariableResolver"]

