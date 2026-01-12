# Examples

## Basic Usage

### Scanning a Repository

```bash
lineage-scan \
  --repo /path/to/spark/repository \
  --out output/lineage_results \
  --config config/lineage_config.yaml
```

### With Hive Metastore

```bash
lineage-scan \
  --repo /path/to/repository \
  --hive-metastore jdbc:hive2://hive-server:10000/default \
  --out output/
```

## Configuration Examples

### Basic Configuration (`lineage_config.yaml`)

```yaml
repo_path: /path/to/repository
output_dir: output/lineage

ignore_patterns:
  paths:
    - "/tmp/**"
    - "**/_logs/**"
    - "**/target/**"

environments:
  production:
    name: "production"
    hdfs_namenode: "hdfs://prod-namenode:8020"
    base_dirs:
      raw: "/prod/data/raw"
      processed: "/prod/data/processed"

active_environment: "production"

export:
  json: true
  csv: true
  html: true
  database: false
```

### Database Export Configuration

```yaml
database:
  enabled: true
  driver: "postgresql"
  host: "localhost"
  port: 5432
  database: "lineage"
  username: "lineage_user"
  password: "secure_password"
  schema: "lineage"

export:
  json: true
  csv: true
  database: true
  html: true
```

## Programming Interface

### Direct API Usage

```python
from pathlib import Path
from lineage.crawler import FileCrawler
from lineage.extractors import PySparkExtractor
from lineage.ir import FactStore
from lineage.rules import create_default_engine
from lineage.resolution import SymbolTable, PathCanonicalizer, VariableResolver
from lineage.lineage import LineageBuilder
from lineage.scoring import PriorityCalculator
from lineage.exporters import JSONExporter

# Setup
repo_path = Path("/path/to/repository")
fact_store = FactStore()
rule_engine = create_default_engine()

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

### Custom Extractor

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

### Custom Rules

`config/custom_rules.yaml`:
```yaml
rules:
  - rule_id: custom_read_api
    applies_to: [pyspark]
    pattern: 'DataReader\.load\(["\'](?P<path>[^"\']+)["\']\)'
    action: READ_HDFS_PATH
    confidence: 0.80
    description: "Custom DataReader API"
  
  - rule_id: custom_write_api
    applies_to: [pyspark]
    pattern: 'DataWriter\.save\([^,]+,\s*["\'](?P<path>[^"\']+)["\']\)'
    action: WRITE_HDFS_PATH
    confidence: 0.80
    description: "Custom DataWriter API"
```

Load custom rules:
```python
from lineage.rules import RuleEngine

engine = RuleEngine()
engine.load_default_rules()
engine.load_rules_from_yaml(Path("config/custom_rules.yaml"))
```

## Output Examples

### JSON Output Structure

```json
{
  "metadata": {
    "total_nodes": 150,
    "total_edges": 245,
    "dataset_nodes": 100,
    "job_nodes": 45,
    "module_nodes": 5
  },
  "nodes": [
    {
      "node_id": "abc123",
      "node_type": "DATASET",
      "urn": "hdfs:///data/processed/users",
      "name": "users",
      "metadata": {
        "dataset_type": "hdfs",
        "fully_resolved": true
      }
    }
  ],
  "edges": [
    {
      "edge_id": "edge123",
      "source_node_id": "node_abc",
      "target_node_id": "node_def",
      "edge_type": "PRODUCES",
      "confidence": 0.85
    }
  ],
  "metrics": {
    "node_abc": {
      "fan_in": 2,
      "fan_out": 5,
      "downstream_reach": 25,
      "priority_score": 75.3,
      "migration_wave": 1
    }
  }
}
```

### CSV Output

**nodes.csv**:
```csv
node_id,node_type,urn,name,metadata
abc123,DATASET,hdfs:///data/raw/users,users,"{""dataset_type"":""hdfs""}"
def456,JOB,job:///jobs/process_users.py,process_users.py,"{""module"":""jobs""}"
```

**edges.csv**:
```csv
edge_id,source_node_id,target_node_id,edge_type,confidence,evidence
e1,abc123,def456,READ,0.85,"spark.read.parquet(...)"
e2,def456,ghi789,WRITE,0.85,"df.write.parquet(...)"
```

**metrics.csv**:
```csv
node_id,fan_in,fan_out,downstream_reach,priority_score,migration_wave,avg_confidence
abc123,0,3,15,45.5,2,0.82
```

## Common Use Cases

### 1. Migration Assessment

Identify high-priority datasets for migration:
```bash
lineage-scan --repo /legacy/codebase --out assessment/
```

Check Wave 1 datasets in `lineage_report.html` or `metrics.csv`.

### 2. Impact Analysis

Find all downstream dependencies of a dataset:
```python
# After building graph
dataset_node = graph.get_node_by_urn("hdfs:///data/raw/critical_data")
downstream = graph.get_transitive_downstream(dataset_node.node_id)
print(f"Impacted datasets: {len(downstream)}")
```

### 3. Confidence Audit

Find low-confidence edges that need manual review:
```python
low_confidence_edges = [
    e for e in graph.edges if e.confidence < 0.6
]
```

### 4. Shell-to-Spark Migration

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

## Troubleshooting

### No facts extracted

- Check file type detection: `FileCrawler.get_stats()`
- Verify extractors are configured correctly
- Check log output for parsing errors

### Low confidence scores

- Add custom rules for organization-specific patterns
- Improve variable resolution by adding config files
- Use AST-based extractors where possible

### Missing lineage

- Check ignore patterns in configuration
- Verify paths are not in temporary/staging directories
- Enable debug logging to see extraction details

### Performance issues

- Enable caching: `performance.enable_caching: true`
- Reduce parallel_workers if memory constrained
- Use incremental mode for large repositories

