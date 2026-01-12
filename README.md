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

**Status**: Production-ready MVP with comprehensive test coverage (>80%)  
**Version**: 0.1.0  
**Last Updated**: January 2026

