"""Confidence scoring for lineage edges and facts."""

from typing import Dict, Any
from lineage.ir import Fact, ExtractionMethod
from lineage.lineage import Edge


class ConfidenceScorer:
    """Calculates confidence scores for lineage elements."""
    
    # Base confidence by extraction method
    METHOD_CONFIDENCE = {
        ExtractionMethod.AST: 0.85,
        ExtractionMethod.SQL_PARSE: 0.85,
        ExtractionMethod.JSON_PARSE: 0.75,
        ExtractionMethod.REGEX: 0.60,
        ExtractionMethod.HEURISTIC: 0.40
    }
    
    def score_fact(self, fact: Fact) -> float:
        """Calculate confidence score for a fact."""
        # Start with base confidence
        confidence = fact.confidence or self.METHOD_CONFIDENCE.get(
            fact.extraction_method, 0.50
        )
        
        # Boost if fully resolved (no placeholders)
        if not fact.has_placeholders:
            confidence += 0.10
        else:
            confidence -= 0.10
        
        # Reduce if ambiguous
        if fact.is_ambiguous:
            confidence -= 0.10
        
        # Reduce if regex-only
        if fact.extraction_method == ExtractionMethod.REGEX:
            confidence -= 0.05
        
        # Clamp to [0, 1]
        return max(0.0, min(1.0, confidence))
    
    def score_edge(self, edge: Edge, facts: list[Fact]) -> float:
        """Calculate confidence score for an edge based on supporting facts."""
        if not facts:
            return edge.confidence
        
        # Average confidence of supporting facts
        avg_confidence = sum(self.score_fact(f) for f in facts) / len(facts)
        
        # Weight with edge's own confidence
        return (avg_confidence + edge.confidence) / 2
    
    def adjust_confidence(
        self,
        base_confidence: float,
        has_placeholders: bool,
        is_regex_only: bool,
        is_ambiguous: bool,
        is_resolved: bool
    ) -> float:
        """Adjust confidence based on various factors."""
        confidence = base_confidence
        
        if is_resolved:
            confidence += 0.10
        elif has_placeholders:
            confidence -= 0.10
        
        if is_regex_only:
            confidence -= 0.20
        
        if is_ambiguous:
            confidence -= 0.10
        
        return max(0.0, min(1.0, confidence))

