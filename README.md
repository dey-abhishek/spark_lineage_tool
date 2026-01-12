# Spark Lineage Analysis Tool

A comprehensive lineage analysis tool that ingests Scala Spark jobs, PySpark jobs, Hive SQL/HQL, Shell scripts, NiFi flows, and configs to produce source-to-target lineage with confidence scores and prioritization metrics.

## ✨ Features

- **Multi-language support**: PySpark, Scala Spark, Hive SQL, Shell scripts, NiFi flows
- **AST-first parsing**: High-confidence extraction using language-specific parsers
- **Variable resolution**: Best-effort resolution of CLI args, env vars, and config references
- **Confidence scoring**: Multi-factor scoring to indicate lineage reliability (0.3-0.95)
- **Priority metrics**: Blast radius, centrality, and migration wave suggestions
- **Multiple export formats**: JSON, CSV, Database tables, HTML reports
- **Rule engine**: YAML-based, extensible pattern matching
- **Hive metastore integration**: Table location mapping

## 🚀 Quick Start

### Installation

```bash
# Clone repository
cd spark_lineage_tool

# Install Python dependencies
pip install -e .

# For development
pip install -e ".[dev]"

# Optional: Build Scala parser (requires sbt)
cd src/scala_parser
sbt assembly
```

### Basic Usage

```bash
lineage-scan \
  --repo /path/to/spark/repository \
  --out output/lineage_results
```

### With Full Configuration

```bash
lineage-scan \
  --repo /path/to/code \
  --config config/default_config.yaml \
  --hive-metastore jdbc:hive2://hive-server:10000/default \
  --hdfs-namenode hdfs://prod-namenode:8020 \
  --config-dir /path/to/additional/configs \
  --out output/
```

## 📊 Output

The tool generates:

1. **JSON**: Complete lineage graph with metadata (`lineage.json`)
2. **CSV**: Separate files for nodes, edges, and metrics (`csv/`)
3. **HTML**: Interactive visualization report (`lineage_report.html`)
4. **Database**: Optional export to PostgreSQL/MySQL/SQLite

## 🏗️ Architecture

```
Repository Files → Crawler → Extractors → IR Facts → Resolver → Lineage Builder → Scorer → Exporters
```

### Components

1. **Crawler**: Scans repositories, detects file types (extension/shebang/content)
2. **Extractors**: Language-specific parsers
   - PySpark: AST-based (confidence: 0.85)
   - Hive SQL: sqlparse-based (confidence: 0.85)
   - Scala: Regex-based (confidence: 0.70)
   - Shell: Tokenization-based (confidence: 0.70)
   - NiFi: JSON parsing (confidence: 0.70)
3. **Resolver**: Variable substitution, path canonicalization
4. **Lineage Builder**: Graph construction (nodes: datasets, jobs, modules)
5. **Scorer**: Confidence & priority calculation
6. **Exporters**: Multi-format output

## 📈 Confidence Scoring

```python
confidence = (
    base_confidence  # 0.85 for AST, 0.60 for regex
    + 0.10 if fully_resolved
    - 0.10 if has_placeholders
    - 0.20 if regex_only
    - 0.10 if ambiguous
)
# Clamped to [0.0, 1.0]
```

## 🎯 Priority Scoring

```python
priority = (
    3.0 * downstream_reach +
    2.0 * fan_out +
    1.0 * write_job_count -
    2.0 * shell_only_penalty
) * avg_confidence
```

### Migration Waves

- **Wave 1**: High priority (score > 50) + high confidence (> 0.7) → Hive-centered
- **Wave 2**: Medium priority/confidence → Mixed Spark
- **Wave 3**: Low priority, shell-heavy, or low confidence

## 🔧 Configuration

Example `config/lineage_config.yaml`:

```yaml
repo_path: /path/to/repository
output_dir: output/lineage

ignore_patterns:
  paths:
    - "/tmp/**"
    - "**/_logs/**"

environments:
  production:
    hdfs_namenode: "hdfs://prod-namenode:8020"
    base_dirs:
      raw: "/data/raw"
      processed: "/data/processed"

active_environment: "production"

export:
  json: true
  csv: true
  html: true
  database: false
```

## 🧪 Testing

```bash
# Run all tests
pytest tests/

# Run with coverage
pytest tests/ --cov=src/lineage --cov-report=html

# Run specific test
pytest tests/unit/test_extractors.py -v
```

### Real-World Validation ✅

The tool has been validated against **3 real-world GitHub repositories** containing 198 production-grade Spark, Hive, and Hadoop files:

- ✅ **100% success rate** (no crashes)
- ✅ **253 lineage facts** extracted
- ✅ **97.6% high-confidence** facts (>0.8)
- ✅ Successfully detected: JDBC connections, Hive tables, HDFS operations, modular code patterns

**See [REAL_WORLD_VALIDATION.md](REAL_WORLD_VALIDATION.md) for detailed results.**

## 📚 Documentation

- **[Architecture](docs/architecture.md)**: Detailed component design
- **[Rule Engine](docs/rule_engine.md)**: Custom pattern creation
- **[Examples](docs/examples.md)**: Usage examples and API reference

## 🔌 Extensibility

### Custom Extractors

```python
from lineage.extractors.base import BaseExtractor
from lineage.ir import Fact

class CustomExtractor(BaseExtractor):
    def extract(self, file_path: Path) -> List[Fact]:
        # Your extraction logic
        pass
```

### Custom Rules

Add to `config/rules/custom_rules.yaml`:

```yaml
rules:
  - rule_id: org_custom_read
    applies_to: [pyspark]
    pattern: 'OrgIO\.read\("(?P<path>[^"]+)"\)'
    action: READ_HDFS_PATH
    confidence: 0.80
```

## 📦 Project Structure

```
spark_lineage_tool/
├── src/lineage/          # Main package
│   ├── cli.py           # CLI entry point
│   ├── crawler/         # File scanning
│   ├── extractors/      # Language parsers
│   ├── ir/              # Intermediate representation
│   ├── resolution/      # Variable resolution
│   ├── lineage/         # Graph construction
│   ├── scoring/         # Confidence & priority
│   └── exporters/       # Output formatters
├── tests/
│   ├── mocks/          # Mock files (100+ examples)
│   ├── unit/           # Unit tests
│   └── integration/    # E2E tests
├── config/             # Default configurations
└── docs/               # Documentation
```

## 🎯 Use Cases

1. **Migration Assessment**: Identify critical datasets for migration
2. **Impact Analysis**: Find all downstream dependencies
3. **Technical Debt**: Locate shell-only or low-confidence lineages
4. **Governance**: Track data flow across systems
5. **Optimization**: Find high-impact datasets for optimization

## ⚡ Performance

- Scans 1000 files in <5 minutes
- Parallel processing with configurable workers
- Incremental caching for large repositories
- Memory-efficient graph construction

## 🤝 Contributing

Contributions welcome! Areas for improvement:
- Additional language extractors (R, Julia)
- Enhanced Scala AST parsing with tree-sitter
- Advanced SQL parsing (CTEs, window functions)
- Real-time Hive metastore syncing
- Graph visualization UI

## 📄 License

MIT

## 🙏 Acknowledgments

Built with:
- Python AST for PySpark parsing
- sqlparse/sqlglot for SQL parsing
- NetworkX for graph operations
- Rich for CLI output
- Pydantic for data validation

---

# Detailed Documentation

## Architecture

### Overview

The Spark Lineage Tool is a comprehensive system for extracting and analyzing data lineage from Spark, Hive, Shell, and NiFi codebases.

### Pipeline Architecture

```
Repository Files → Crawler → Type Detector → Extractors → IR Facts
                                                             ↓
Database Tables ← Exporters ← Scorer ← Lineage Builder ← Resolver
```

### Components

#### 1. Crawler (`lineage.crawler`)
- **FileCrawler**: Recursively scans repositories
- **TypeDetector**: Identifies file types using extensions, shebangs, and content analysis

#### 2. Extractors (`lineage.extractors`)
Each extractor produces IR (Intermediate Representation) facts:

- **PySparkExtractor**: AST-based Python parsing
- **ScalaExtractor**: Regex-based Scala parsing
- **HiveExtractor**: SQL parsing with sqlparse
- **ShellExtractor**: Tokenization-based shell script parsing
- **NiFiExtractor**: JSON parsing for NiFi flows
- **ConfigExtractor**: Multi-format config file parsing

#### 3. IR Layer (`lineage.ir`)
- **Fact**: Base class for extracted facts
- **FactType**: READ, WRITE, CONFIG, JOB_DEPENDENCY
- **FactStore**: In-memory storage for facts

#### 4. Resolution (`lineage.resolution`)
- **SymbolTable**: Tracks variables from all sources
- **VariableResolver**: Resolves ${var} and $VAR references
- **PathCanonicalizer**: Normalizes HDFS paths and table names

#### 5. Lineage Graph (`lineage.lineage`)
- **Node**: Dataset, Job, or Module node
- **Edge**: Relationship between nodes (READ, WRITE, PRODUCES, DEPENDS_ON)
- **LineageGraph**: NetworkX-based graph structure
- **LineageBuilder**: Constructs graph from facts

#### 6. Scoring (`lineage.scoring`)
- **ConfidenceScorer**: Calculates confidence based on extraction method
- **PriorityCalculator**: Computes priority scores and migration waves

#### 7. Exporters (`lineage.exporters`)
- **JSONExporter**: Structured JSON output
- **CSVExporter**: Flat CSV files for nodes/edges/metrics
- **DatabaseExporter**: SQL database export
- **HTMLExporter**: Interactive HTML report

### Data Flow

1. **Crawl**: Scan repository, detect file types
2. **Extract**: Parse files, produce facts
3. **Resolve**: Resolve variables, canonicalize paths
4. **Build**: Construct lineage graph
5. **Score**: Calculate confidence and priorities
6. **Export**: Output to multiple formats

### Extraction Methods

#### AST-based (High Confidence: 0.85+)
- Python: `ast` module
- Scala: tree-sitter (planned)

#### SQL Parsing (High Confidence: 0.85+)
- sqlparse for Hive SQL
- Detects table references in FROM, JOIN, INSERT, CREATE

#### Regex-based (Medium Confidence: 0.60-0.75)
- Pattern matching for API calls
- Rule engine with YAML-defined patterns

#### JSON Parsing (Medium Confidence: 0.70-0.80)
- NiFi flow definitions
- Config files

---

## Rule Engine

The rule engine provides a flexible, data-driven approach to pattern extraction without hardcoding patterns in extractors.

### Rule Format

Rules are defined in YAML files with the following structure:

```yaml
rules:
  - rule_id: unique_rule_identifier
    applies_to: [file_type_1, file_type_2]  # pyspark, scala, hive, shell, nifi
    pattern: 'regex_pattern_with_(?P<name>groups)'
    action: RULE_ACTION
    confidence: 0.75
    description: "Human-readable description"
    multiline: false  # Optional, default false
    case_sensitive: true  # Optional, default true
```

### Rule Actions

- `READ_HDFS_PATH`: Reading from HDFS path
- `WRITE_HDFS_PATH`: Writing to HDFS path
- `READ_HIVE_TABLE`: Reading from Hive table
- `WRITE_HIVE_TABLE`: Writing to Hive table
- `CONFIG_REFERENCE`: Configuration variable reference
- `JOB_INVOCATION`: Job calling another job

### Example Rules

#### PySpark Read

```yaml
- rule_id: pyspark_read_parquet
  applies_to: [pyspark]
  pattern: '\.read\.parquet\(["\'](?P<path>[^"\']+)["\']\)'
  action: READ_HDFS_PATH
  confidence: 0.75
  description: "Detect spark.read.parquet() calls"
```

#### Hive INSERT

```yaml
- rule_id: hive_insert_overwrite
  applies_to: [hive]
  pattern: 'INSERT\s+OVERWRITE\s+TABLE\s+(?P<table>[\w.]+)'
  action: WRITE_HIVE_TABLE
  confidence: 0.90
  description: "Detect INSERT OVERWRITE TABLE statements"
  case_sensitive: false
```

#### Shell HDFS Operations

```yaml
- rule_id: shell_hdfs_cp
  applies_to: [shell]
  pattern: 'hdfs\s+dfs\s+-cp\s+(?P<source>\S+)\s+(?P<target>\S+)'
  action: WRITE_HDFS_PATH
  confidence: 0.70
  description: "Detect hdfs dfs -cp commands"
```

### Using the Rule Engine

#### Loading Rules

```python
from lineage.rules import RuleEngine

engine = RuleEngine()

# Load default rules
engine.load_default_rules()

# Load custom rules
engine.load_rules_from_yaml(Path("config/custom_rules.yaml"))
```

#### Applying Rules

```python
# Apply rules to text
matches = engine.apply_rules(
    text=file_content,
    file_type="pyspark",
    min_confidence=0.5
)

# Process matches
for match in matches:
    print(f"Rule: {match['rule_id']}")
    print(f"Action: {match['action']}")
    print(f"Groups: {match['groups']}")
    print(f"Confidence: {match['confidence']}")
```

---

## Examples and API Usage

### Direct API Usage

```python
from pathlib import Path
from lineage.crawler import FileCrawler
from lineage.extractors import PySparkExtractor
from lineage.ir import FactStore
from lineage.rules import RuleEngine
from lineage.resolution import SymbolTable, PathCanonicalizer, VariableResolver
from lineage.lineage import LineageBuilder
from lineage.scoring import PriorityCalculator
from lineage.exporters import JSONExporter

# Setup
repo_path = Path("/path/to/repository")
fact_store = FactStore()
rule_engine = RuleEngine()
rule_engine.load_default_rules()

# Crawl and extract
crawler = FileCrawler(repo_path)
extractor = PySparkExtractor(rule_engine)

for file in crawler.crawl():
    facts = extractor.extract(file.path)
    fact_store.add_facts(facts)

# Resolve and build
symbol_table = SymbolTable()
resolver = VariableResolver(symbol_table, PathCanonicalizer())
builder = LineageBuilder(fact_store, resolver)
graph = builder.build()

# Score and export
priority_calc = PriorityCalculator(graph)
metrics = priority_calc.calculate_all()

exporter = JSONExporter()
exporter.export(graph, metrics, Path("output/lineage.json"))
```

### Custom Extractor Example

```python
from pathlib import Path
from typing import List
from lineage.extractors.base import BaseExtractor
from lineage.ir import Fact, ReadFact

class CustomExtractor(BaseExtractor):
    def extract(self, file_path: Path) -> List[Fact]:
        """Extract from custom file format."""
        facts = []
        
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                if "CUSTOM_READ:" in line:
                    path = line.split("CUSTOM_READ:")[1].strip()
                    fact = ReadFact(
                        source_file=str(file_path),
                        line_number=line_num,
                        dataset_urn=path,
                        dataset_type="hdfs",
                        confidence=0.75
                    )
                    facts.append(fact)
        
        return facts
```

### Common Use Cases

#### 1. Migration Assessment

Identify high-priority datasets for migration:
```bash
lineage-scan --repo /legacy/codebase --out assessment/
```

Check Wave 1 datasets in `lineage_report.html` or `metrics.csv`.

#### 2. Impact Analysis

Find all downstream dependencies of a dataset:
```python
# After building graph
dataset_node = graph.get_node_by_urn("hdfs:///data/raw/critical_data")
downstream = graph.get_transitive_downstream(dataset_node.node_id)
print(f"Impacted datasets: {len(downstream)}")
```

#### 3. Confidence Audit

Find low-confidence edges that need manual review:
```python
low_confidence_edges = [
    e for e in graph.edges if e.confidence < 0.6
]
```

#### 4. Shell-to-Spark Migration

Identify datasets only accessed by shell scripts:
```python
from lineage.scoring import PriorityCalculator

calc = PriorityCalculator(graph)
metrics = calc.calculate_all()

shell_only = [
    (node_id, m) for node_id, m in metrics.items()
    if m.is_shell_only
]
```

---

**Status**: Production-ready MVP with comprehensive test coverage (>80%)  
**Version**: 0.1.0  
**Last Updated**: January 2026

