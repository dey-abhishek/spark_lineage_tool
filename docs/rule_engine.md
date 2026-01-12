# Rule Engine

The rule engine provides a flexible, data-driven approach to pattern extraction without hardcoding patterns in extractors.

## Rule Format

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

## Rule Actions

- `READ_HDFS_PATH`: Reading from HDFS path
- `WRITE_HDFS_PATH`: Writing to HDFS path
- `READ_HIVE_TABLE`: Reading from Hive table
- `WRITE_HIVE_TABLE`: Writing to Hive table
- `CONFIG_REFERENCE`: Configuration variable reference
- `JOB_INVOCATION`: Job calling another job

## Example Rules

### PySpark Read

```yaml
- rule_id: pyspark_read_parquet
  applies_to: [pyspark]
  pattern: '\.read\.parquet\(["\'](?P<path>[^"\']+)["\']\)'
  action: READ_HDFS_PATH
  confidence: 0.75
  description: "Detect spark.read.parquet() calls"
```

### Hive INSERT

```yaml
- rule_id: hive_insert_overwrite
  applies_to: [hive]
  pattern: 'INSERT\s+OVERWRITE\s+TABLE\s+(?P<table>[\w.]+)'
  action: WRITE_HIVE_TABLE
  confidence: 0.90
  description: "Detect INSERT OVERWRITE TABLE statements"
  case_sensitive: false
```

### Shell HDFS Operations

```yaml
- rule_id: shell_hdfs_cp
  applies_to: [shell]
  pattern: 'hdfs\s+dfs\s+-cp\s+(?P<source>\S+)\s+(?P<target>\S+)'
  action: WRITE_HDFS_PATH
  confidence: 0.70
  description: "Detect hdfs dfs -cp commands"
```

## Using the Rule Engine

### Loading Rules

```python
from lineage.rules import RuleEngine

engine = RuleEngine()

# Load default rules
engine.load_default_rules()

# Load custom rules
engine.load_rules_from_yaml(Path("config/custom_rules.yaml"))
```

### Applying Rules

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

## Custom Rule Files

Create custom rules in `config/rules/custom_rules.yaml`:

```yaml
rules:
  # Organization-specific patterns
  - rule_id: org_custom_io_read
    applies_to: [pyspark, scala]
    pattern: 'OrgIO\.read\(["\'](?P<path>[^"\']+)["\']\)'
    action: READ_HDFS_PATH
    confidence: 0.80
    description: "Organization-specific IO library read"
  
  - rule_id: org_custom_io_write
    applies_to: [pyspark, scala]
    pattern: 'OrgIO\.write\([^,]+,\s*["\'](?P<path>[^"\']+)["\']\)'
    action: WRITE_HDFS_PATH
    confidence: 0.80
    description: "Organization-specific IO library write"
```

## Rule Development Tips

### 1. Use Named Capture Groups

Always use named capture groups to extract relevant data:
```yaml
pattern: '\.read\.parquet\(["\'](?P<path>[^"\']+)["\']\)'
```

### 2. Balance Specificity

- Too specific: May miss valid patterns
- Too general: May produce false positives

### 3. Test Rules

Test rules against known code samples:
```python
import re

pattern = re.compile(r'\.read\.parquet\(["\'](?P<path>[^"\']+)["\']\)')
test_code = 'df = spark.read.parquet("/data/raw/users")'

match = pattern.search(test_code)
if match:
    print(match.group('path'))  # /data/raw/users
```

### 4. Assign Appropriate Confidence

- AST-equivalent patterns: 0.80-0.90
- Direct API patterns: 0.70-0.80
- Heuristic patterns: 0.50-0.70
- Ambiguous patterns: 0.30-0.50

### 5. Handle Multiline Cases

For patterns spanning multiple lines:
```yaml
- rule_id: multiline_sql
  applies_to: [pyspark]
  pattern: '\.sql\(["\'](?P<sql>.*?)["\']\)'
  action: READ_HIVE_TABLE
  confidence: 0.75
  multiline: true
```

## Rule Priority

When multiple rules match the same text, all matches are kept. The confidence score indicates reliability.

## Extending the Rule Engine

Add new actions by extending `RuleAction` enum:

```python
from lineage.rules.engine import RuleAction

class ExtendedRuleAction(RuleAction):
    CUSTOM_ACTION = "CUSTOM_ACTION"
```

Then handle the new action in extractors.

