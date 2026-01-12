# Architecture

## Overview

The Spark Lineage Tool is a comprehensive system for extracting and analyzing data lineage from Spark, Hive, Shell, and NiFi codebases.

## Pipeline Architecture

```
Repository Files → Crawler → Type Detector → Extractors → IR Facts
                                                              ↓
Database Tables ← Exporters ← Scorer ← Lineage Builder ← Resolver
```

## Components

### 1. Crawler (`lineage.crawler`)
- **FileCrawler**: Recursively scans repositories
- **TypeDetector**: Identifies file types using extensions, shebangs, and content analysis

### 2. Extractors (`lineage.extractors`)
Each extractor produces IR (Intermediate Representation) facts:

- **PySparkExtractor**: AST-based Python parsing
- **ScalaExtractor**: Regex-based Scala parsing
- **HiveExtractor**: SQL parsing with sqlparse
- **ShellExtractor**: Tokenization-based shell script parsing
- **NiFiExtractor**: JSON parsing for NiFi flows
- **ConfigExtractor**: Multi-format config file parsing

### 3. IR Layer (`lineage.ir`)
- **Fact**: Base class for extracted facts
- **FactType**: READ, WRITE, CONFIG, JOB_DEPENDENCY
- **FactStore**: In-memory storage for facts

### 4. Resolution (`lineage.resolution`)
- **SymbolTable**: Tracks variables from all sources
- **VariableResolver**: Resolves ${var} and $VAR references
- **PathCanonicalizer**: Normalizes HDFS paths and table names

### 5. Lineage Graph (`lineage.lineage`)
- **Node**: Dataset, Job, or Module node
- **Edge**: Relationship between nodes (READ, WRITE, PRODUCES, DEPENDS_ON)
- **LineageGraph**: NetworkX-based graph structure
- **LineageBuilder**: Constructs graph from facts

### 6. Scoring (`lineage.scoring`)
- **ConfidenceScorer**: Calculates confidence based on extraction method
- **PriorityCalculator**: Computes priority scores and migration waves

### 7. Exporters (`lineage.exporters`)
- **JSONExporter**: Structured JSON output
- **CSVExporter**: Flat CSV files for nodes/edges/metrics
- **DatabaseExporter**: SQL database export
- **HTMLExporter**: Interactive HTML report

## Data Flow

1. **Crawl**: Scan repository, detect file types
2. **Extract**: Parse files, produce facts
3. **Resolve**: Resolve variables, canonicalize paths
4. **Build**: Construct lineage graph
5. **Score**: Calculate confidence and priorities
6. **Export**: Output to multiple formats

## Extraction Methods

### AST-based (High Confidence: 0.85+)
- Python: `ast` module
- Scala: tree-sitter (planned)

### SQL Parsing (High Confidence: 0.85+)
- sqlparse for Hive SQL
- Detects table references in FROM, JOIN, INSERT, CREATE

### Regex-based (Medium Confidence: 0.60-0.75)
- Pattern matching for API calls
- Rule engine with YAML-defined patterns

### JSON Parsing (Medium Confidence: 0.70-0.80)
- NiFi flow definitions
- Config files

## Confidence Calculation

```python
confidence = (
    base_confidence  # From extraction method
    + 0.10 if fully_resolved else 0
    - 0.10 if has_placeholders else 0
    - 0.20 if regex_only else 0
    - 0.10 if ambiguous else 0
)
# Clamped to [0, 1]
```

## Priority Scoring

```python
priority_score = (
    3.0 * downstream_reach
    + 2.0 * fan_out
    + 1.0 * write_job_count
    - 2.0 * shell_only_penalty
) * avg_confidence
```

## Migration Waves

- **Wave 1**: High priority (score > 50) + high confidence (> 0.7)
- **Wave 2**: Medium priority or confidence
- **Wave 3**: Low priority, shell-heavy, or low confidence

## Extension Points

### Custom Extractors
Implement `BaseExtractor` interface:
```python
from lineage.extractors.base import BaseExtractor

class CustomExtractor(BaseExtractor):
    def extract(self, file_path: Path) -> List[Fact]:
        # Implementation
        pass
```

### Custom Rules
Add to YAML rule file:
```yaml
- rule_id: custom_pattern
  applies_to: [pyspark]
  pattern: '\.customRead\("(?P<path>[^"]+)"\)'
  action: READ_HDFS_PATH
  confidence: 0.75
```

### Custom Scorers
Extend `ConfidenceScorer` or `PriorityCalculator`:
```python
from lineage.scoring import PriorityCalculator

class CustomPriorityCalculator(PriorityCalculator):
    def calculate_node_metrics(self, node):
        # Custom logic
        pass
```

