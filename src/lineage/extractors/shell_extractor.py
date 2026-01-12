"""Shell script tokenizer and extractor."""

from pathlib import Path
from typing import List, Optional, Dict, Any
import re
import shlex

from .base import BaseExtractor
from lineage.ir import Fact, ReadFact, WriteFact, JobDependencyFact, ConfigFact, ExtractionMethod
from lineage.rules import RuleEngine


class ShellExtractor(BaseExtractor):
    """Extractor for shell scripts."""
    
    def __init__(self, rule_engine: Optional[RuleEngine] = None) -> None:
        super().__init__()
        self.rule_engine = rule_engine
        
        # Shell command patterns
        self.patterns = {
            "hdfs_get": re.compile(r'hdfs\s+dfs\s+-get\s+(\S+)\s+(\S+)'),
            "hdfs_put": re.compile(r'hdfs\s+dfs\s+-put\s+(\S+)\s+(\S+)'),
            "hdfs_cp": re.compile(r'hdfs\s+dfs\s+-cp\s+(\S+)\s+(\S+)'),
            "hdfs_mv": re.compile(r'hdfs\s+dfs\s+-mv\s+(\S+)\s+(\S+)'),
            "hdfs_cat": re.compile(r'hdfs\s+dfs\s+-cat\s+(\S+)'),
            "hdfs_text": re.compile(r'hdfs\s+dfs\s+-text\s+(\S+)'),
            "distcp": re.compile(r'hadoop\s+distcp\s+(?:(?:-\w+\s+)+)?(\S+)\s+(\S+)'),
            "spark_submit": re.compile(r'spark-submit\s+.*?(\S+\.(?:py|jar|scala))'),
            "hive_execute": re.compile(r'hive\s+-e\s+["\']([^"\']+)["\']'),
            "hive_file": re.compile(r'hive\s+-f\s+(\S+)'),
            "beeline_execute": re.compile(r'beeline\s+.*?-e\s+["\']([^"\']+)["\']', re.DOTALL),
            "beeline_file": re.compile(r'beeline\s+.*?-f\s+(\S+)'),
        }
    
    def extract(self, file_path: Path) -> List[Fact]:
        """Extract facts from shell script."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            return self.extract_from_content(content, str(file_path))
        
        except Exception as e:
            print(f"Error extracting from {file_path}: {e}")
            return []
    
    def extract_from_content(self, content: str, source_file: str) -> List[Fact]:
        """Extract facts from shell script content."""
        facts = []
        
        # Remove comments
        content_no_comments = self._remove_comments(content)
        
        # Join lines with backslash continuation
        content_joined = self._join_continued_lines(content_no_comments)
        
        # Extract variable definitions (export VAR=value)
        facts.extend(self._extract_variable_definitions(content_joined, source_file))
        
        # Extract HDFS operations
        facts.extend(self._extract_hdfs_ops(content_joined, source_file))
        
        # Extract job invocations
        facts.extend(self._extract_job_invocations(content_joined, source_file))
        
        # Use rule engine for additional patterns
        if self.rule_engine:
            matches = self.rule_engine.apply_rules(content_joined, "shell")
            for match in matches:
                fact = self._match_to_fact(match, source_file)
                if fact:
                    facts.append(fact)
        
        return facts
    
    def _join_continued_lines(self, content: str) -> str:
        """Join lines that end with backslash continuation."""
        lines = content.split("\n")
        joined_lines = []
        current_line = ""
        
        for line in lines:
            if line.rstrip().endswith("\\"):
                # Remove trailing backslash and whitespace, add to current line
                current_line += line.rstrip()[:-1].rstrip() + " "
            else:
                # Add this line and commit the accumulated line
                current_line += line
                joined_lines.append(current_line)
                current_line = ""
        
        # Add any remaining line
        if current_line:
            joined_lines.append(current_line)
        
        return "\n".join(joined_lines)
    
    def _remove_comments(self, content: str) -> str:
        """Remove shell comments."""
        lines = []
        for line in content.split("\n"):
            # Remove inline comments (but preserve # in strings)
            if "#" in line:
                # Simple heuristic: if # is not in quotes, treat as comment
                comment_pos = line.find("#")
                before_comment = line[:comment_pos]
                # Count quotes before comment
                single_quotes = before_comment.count("'")
                double_quotes = before_comment.count('"')
                # If even number of quotes, # starts a comment
                if single_quotes % 2 == 0 and double_quotes % 2 == 0:
                    line = line[:comment_pos]
            lines.append(line)
        return "\n".join(lines)
    
    def _extract_variable_definitions(self, content: str, source_file: str) -> List[Fact]:
        """Extract variable definitions from shell script (export VAR=value, VAR=value)."""
        facts = []
        
        # Pattern to match export statements and variable assignments
        # Matches: export VAR=value, VAR=value, VAR="value", VAR='value', VAR=${OTHER}, VAR=$(cmd)
        # Handle quotes carefully - match either quoted strings or unquoted values
        # For quoted: match everything until the closing quote
        # For unquoted: match until newline or comment
        export_pattern = re.compile(
            r'^(?:export\s+)?(\w+)=((["\'])(.+?)\3|([^#\n]+?)(?:\s*#|$))',
            re.MULTILINE
        )
        
        for match in export_pattern.finditer(content):
            var_name = match.group(1)
            # Group 2 is the full value (either quoted or unquoted)
            # Group 4 is quoted content (if quoted)
            # Group 5 is unquoted content (if not quoted)
            value = (match.group(4) or match.group(5) or '').strip()
            line_number = content[:match.start()].count("\n") + 1
            
            # Try to resolve date expressions like $(date +%Y-%m-%d) or `date +%Y-%m-%d`
            resolved_value = self._resolve_date_expression(value)
            
            # Also resolve parameter defaults: ${1:-default} -> default
            resolved_value = self._resolve_parameter_defaults(resolved_value)
            
            confidence = 0.85
            
            # Lower confidence if it still contains unresolved placeholders
            if "${" in resolved_value or "$(" in resolved_value:
                confidence = 0.70
            
            # Create a ConfigFact for the variable definition
            fact = ConfigFact(
                source_file=source_file,
                line_number=line_number,
                config_key=var_name,
                config_value=resolved_value,
                config_source="shell_export",
                extraction_method=ExtractionMethod.REGEX,
                confidence=confidence
            )
            facts.append(fact)
        
        return facts
    
    def _resolve_date_expression(self, value: str) -> str:
        """Resolve date command expressions to actual current date values."""
        from datetime import datetime
        
        # Get current date
        now = datetime.now()
        
        # Pattern mappings to actual date formats (including timestamps)
        # NOTE: Focusing on filesystem-safe formats (no colons for Windows compatibility)
        date_patterns = [
            # Date only formats
            (r'\$\(date\s+\+%Y-%m-%d\)', now.strftime('%Y-%m-%d')),  # 2024-01-15
            (r'`date\s+\+%Y-%m-%d`', now.strftime('%Y-%m-%d')),
            (r'\$\(date\s+\+%Y%m%d\)', now.strftime('%Y%m%d')),  # 20240115
            (r'`date\s+\+%Y%m%d`', now.strftime('%Y%m%d')),
            (r'\$\(date\s+\+%Y\)', now.strftime('%Y')),  # 2024
            (r'`date\s+\+%Y`', now.strftime('%Y')),
            (r'\$\(date\s+\+%m\)', now.strftime('%m')),  # 01
            (r'`date\s+\+%m`', now.strftime('%m')),
            (r'\$\(date\s+\+%d\)', now.strftime('%d')),  # 15
            (r'`date\s+\+%d`', now.strftime('%d')),
            (r'\$\(date\s+\+%Y-%m\)', now.strftime('%Y-%m')),  # 2024-01
            (r'`date\s+\+%Y-%m`', now.strftime('%Y-%m')),
            (r'\$\(date\s+\+%Y/%m/%d\)', now.strftime('%Y/%m/%d')),  # 2024/01/15 (for paths)
            (r'`date\s+\+%Y/%m/%d`', now.strftime('%Y/%m/%d')),
            (r'\$\(date\s+\+%Y\.%m\.%d\)', now.strftime('%Y.%m.%d')),  # 2024.01.15
            (r'`date\s+\+%Y\.%m\.%d`', now.strftime('%Y.%m.%d')),
            
            # Timestamp formats (Date + Time) - FILESYSTEM SAFE (no colons)
            (r'\$\(date\s+\+%Y%m%d%H%M%S\)', now.strftime('%Y%m%d%H%M%S')),  # 20240115143025
            (r'`date\s+\+%Y%m%d%H%M%S`', now.strftime('%Y%m%d%H%M%S')),
            (r'\$\(date\s+\+%Y-%m-%d_%H-%M-%S\)', now.strftime('%Y-%m-%d_%H-%M-%S')),  # 2024-01-15_14-30-25
            (r'`date\s+\+%Y-%m-%d_%H-%M-%S`', now.strftime('%Y-%m-%d_%H-%M-%S')),
            (r'\$\(date\s+\+%Y%m%d_%H%M%S\)', now.strftime('%Y%m%d_%H%M%S')),  # 20240115_143025
            (r'`date\s+\+%Y%m%d_%H%M%S`', now.strftime('%Y%m%d_%H%M%S')),
            (r'\$\(date\s+\+%Y%m%d-%H%M%S\)', now.strftime('%Y%m%d-%H%M%S')),  # 20240115-143025
            (r'`date\s+\+%Y%m%d-%H%M%S`', now.strftime('%Y%m%d-%H%M%S')),
            
            # ISO-like format but filesystem safe (T separator, no colons)
            (r'\$\(date\s+\+%Y-%m-%dT%H%M%S\)', now.strftime('%Y-%m-%dT%H%M%S')),  # 2024-01-15T143025
            (r'`date\s+\+%Y-%m-%dT%H%M%S`', now.strftime('%Y-%m-%dT%H%M%S')),
            
            # Time only formats - FILESYSTEM SAFE (dashes instead of colons)
            (r'\$\(date\s+\+%H-%M-%S\)', now.strftime('%H-%M-%S')),  # 14-30-25
            (r'`date\s+\+%H-%M-%S`', now.strftime('%H-%M-%S')),
            (r'\$\(date\s+\+%H%M%S\)', now.strftime('%H%M%S')),  # 143025
            (r'`date\s+\+%H%M%S`', now.strftime('%H%M%S')),
            (r'\$\(date\s+\+%H\)', now.strftime('%H')),  # 14
            (r'`date\s+\+%H`', now.strftime('%H')),
            (r'\$\(date\s+\+%M\)', now.strftime('%M')),  # 30
            (r'`date\s+\+%M`', now.strftime('%M')),
            (r'\$\(date\s+\+%S\)', now.strftime('%S')),  # 25
            (r'`date\s+\+%S`', now.strftime('%S')),
            
            # Unix timestamp (always filesystem safe)
            (r'\$\(date\s+\+%s\)', str(int(now.timestamp()))),  # 1705330225
            (r'`date\s+\+%s`', str(int(now.timestamp()))),
            
            # Milliseconds (sometimes used)
            (r'\$\(date\s+\+%s%3N\)', str(int(now.timestamp() * 1000))),  # 1705330225123
            (r'`date\s+\+%s%3N`', str(int(now.timestamp() * 1000))),
            
            # Default
            (r'\$\(date\)', now.strftime('%Y-%m-%d')),  # Default
            (r'`date`', now.strftime('%Y-%m-%d')),
        ]
        
        result = value
        for pattern, replacement in date_patterns:
            result = re.sub(pattern, replacement, result)
        
        # Handle $(date -d "..." +format) - date arithmetic
        # Examples: $(date -d "${RUN_DATE} -30 days" +%Y-%m-%d)
        # For static analysis, we use a placeholder since we can't calculate relative dates
        date_arithmetic_pattern = r'\$\(date\s+-d\s+[^)]+\+([^)]+)\)'
        def replace_date_arithmetic(match):
            format_spec = match.group(1)
            # Map format to representative value (filesystem-safe)
            if format_spec == '%Y-%m-%d':
                return 'YYYY-MM-DD'
            elif format_spec == '%Y%m%d':
                return 'YYYYMMDD'
            elif format_spec == '%Y%m%d%H%M%S':
                return 'YYYYMMDDHHMMSS'
            elif '%Y-%m-%d_%H-%M-%S' in format_spec:
                return 'YYYY-MM-DD_HH-MM-SS'
            elif '%Y%m%d_%H%M%S' in format_spec:
                return 'YYYYMMDD_HHMMSS'
            elif '%Y%m%d-%H%M%S' in format_spec:
                return 'YYYYMMDD-HHMMSS'
            elif '%Y-%m-%dT%H%M%S' in format_spec:
                return 'YYYY-MM-DDTHHMMSS'  # Filesystem-safe ISO-like
            elif format_spec == '%Y':
                return 'YYYY'
            elif format_spec == '%m':
                return 'MM'
            elif format_spec == '%d':
                return 'DD'
            elif format_spec == '%H-%M-%S':
                return 'HH-MM-SS'
            elif format_spec == '%H%M%S':
                return 'HHMMSS'
            elif format_spec == '%s':
                return 'UNIX_TIMESTAMP'
            else:
                return f'DATE_{format_spec.replace("%", "")}'
        result = re.sub(date_arithmetic_pattern, replace_date_arithmetic, result)
        
        # Handle ${1:-$(date ...)} pattern - parameter with date default
        # Extract the date part and resolve it
        param_date_pattern = r'\$\{(\d+):-\$\(date\s+([^)]+)\)\}'
        def replace_param_date(match):
            param_num = match.group(1)
            date_format = match.group(2)
            # Convert shell date format to Python strftime format
            format_str = date_format.strip()
            if format_str.startswith('+'):
                format_str = format_str[1:]  # Remove leading +
            
            # Map common shell formats to Python strftime
            format_map = {
                '%Y-%m-%d': '%Y-%m-%d',
                '%Y%m%d': '%Y%m%d',
                '%Y/%m/%d': '%Y/%m/%d',
                '%Y.%m.%d': '%Y.%m.%d',
                '%Y-%m': '%Y-%m',
                '%Y': '%Y',
                '%m': '%m',
                '%d': '%d',
            }
            
            python_format = format_map.get(format_str, '%Y-%m-%d')
            return now.strftime(python_format)
        
        result = re.sub(param_date_pattern, replace_param_date, result)
        
        return result
    
    def _resolve_parameter_defaults(self, value: str) -> str:
        """Resolve shell parameter expansion with defaults: ${var:-default} -> default
        
        Extracts default values from parameter expansion syntax.
        Examples:
            ${1:-2024-01-15} -> 2024-01-15
            ${ENV:-prod} -> prod
            ${2:-default} -> default
        """
        # Pattern: ${parameter:-default}
        param_default_pattern = re.compile(r'\$\{([^}:]+):-([^}]+)\}')
        
        def extract_default(match):
            # Always use the default value since we don't have runtime parameters
            default_value = match.group(2)
            # Recursively resolve if the default itself contains parameter expansion
            if '${' in default_value and ':-' in default_value:
                default_value = self._resolve_parameter_defaults(default_value)
            return default_value
        
        return param_default_pattern.sub(extract_default, value)
    
    def _extract_hdfs_ops(self, content: str, source_file: str) -> List[Fact]:
        """Extract HDFS operations."""
        facts = []
        
        # hdfs dfs -get (read)
        for match in self.patterns["hdfs_get"].finditer(content):
            source_path = match.group(1)
            line_number = content[:match.start()].count("\n") + 1
            
            fact = ReadFact(
                source_file=source_file,
                line_number=line_number,
                dataset_urn=source_path,
                dataset_type="hdfs",
                confidence=0.70,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                has_placeholders="$" in source_path
            )
            facts.append(fact)
        
        # hdfs dfs -put (write)
        for match in self.patterns["hdfs_put"].finditer(content):
            target_path = match.group(2)
            line_number = content[:match.start()].count("\n") + 1
            
            fact = WriteFact(
                source_file=source_file,
                line_number=line_number,
                dataset_urn=target_path,
                dataset_type="hdfs",
                confidence=0.70,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                has_placeholders="$" in target_path
            )
            facts.append(fact)
        
        # hdfs dfs -cp (read + write)
        for match in self.patterns["hdfs_cp"].finditer(content):
            source_path = match.group(1)
            target_path = match.group(2)
            line_number = content[:match.start()].count("\n") + 1
            
            # Read from source
            read_fact = ReadFact(
                source_file=source_file,
                line_number=line_number,
                dataset_urn=source_path,
                dataset_type="hdfs",
                confidence=0.70,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                has_placeholders="$" in source_path
            )
            facts.append(read_fact)
            
            # Write to target
            write_fact = WriteFact(
                source_file=source_file,
                line_number=line_number,
                dataset_urn=target_path,
                dataset_type="hdfs",
                confidence=0.70,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                has_placeholders="$" in target_path
            )
            facts.append(write_fact)
        
        # hdfs dfs -mv (read + write, delete source)
        for match in self.patterns["hdfs_mv"].finditer(content):
            source_path = match.group(1)
            target_path = match.group(2)
            line_number = content[:match.start()].count("\n") + 1
            
            read_fact = ReadFact(
                source_file=source_file,
                line_number=line_number,
                dataset_urn=source_path,
                dataset_type="hdfs",
                confidence=0.70,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                has_placeholders="$" in source_path
            )
            facts.append(read_fact)
            
            write_fact = WriteFact(
                source_file=source_file,
                line_number=line_number,
                dataset_urn=target_path,
                dataset_type="hdfs",
                confidence=0.70,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                has_placeholders="$" in target_path
            )
            facts.append(write_fact)
        
        # distcp
        for match in self.patterns["distcp"].finditer(content):
            source_path = match.group(1)
            target_path = match.group(2)
            line_number = content[:match.start()].count("\n") + 1
            
            read_fact = ReadFact(
                source_file=source_file,
                line_number=line_number,
                dataset_urn=source_path,
                dataset_type="hdfs",
                confidence=0.75,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                has_placeholders="$" in source_path
            )
            facts.append(read_fact)
            
            write_fact = WriteFact(
                source_file=source_file,
                line_number=line_number,
                dataset_urn=target_path,
                dataset_type="hdfs",
                confidence=0.75,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                has_placeholders="$" in target_path
            )
            facts.append(write_fact)
        
        return facts
    
    def _extract_job_invocations(self, content: str, source_file: str) -> List[Fact]:
        """Extract job invocations (spark-submit, hive, beeline, etc.)."""
        facts = []
        
        # spark-submit
        for match in self.patterns["spark_submit"].finditer(content):
            script = match.group(1)
            line_number = content[:match.start()].count("\n") + 1
            
            fact = JobDependencyFact(
                source_file=source_file,
                line_number=line_number,
                confidence=0.80,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                dependency_job=script,
                dependency_type="spark-submit"
            )
            facts.append(fact)
        
        # beeline -e
        for match in self.patterns["beeline_execute"].finditer(content):
            sql = match.group(1)
            line_number = content[:match.start()].count("\n") + 1
            
            fact = JobDependencyFact(
                source_file=source_file,
                line_number=line_number,
                confidence=0.75,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                dependency_type="beeline"
            )
            fact.params["sql"] = sql
            facts.append(fact)
        
        # beeline -f
        for match in self.patterns["beeline_file"].finditer(content):
            sql_file = match.group(1)
            line_number = content[:match.start()].count("\n") + 1
            
            fact = JobDependencyFact(
                source_file=source_file,
                line_number=line_number,
                confidence=0.80,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                dependency_job=sql_file,
                dependency_type="beeline"
            )
            facts.append(fact)
        
        # hive -e
        for match in self.patterns["hive_execute"].finditer(content):
            sql = match.group(1)
            line_number = content[:match.start()].count("\n") + 1
            
            fact = JobDependencyFact(
                source_file=source_file,
                line_number=line_number,
                confidence=0.75,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0)[:100],
                dependency_type="hive-execute"
            )
            fact.params["sql"] = sql
            facts.append(fact)
        
        # hive -f
        for match in self.patterns["hive_file"].finditer(content):
            sql_file = match.group(1)
            line_number = content[:match.start()].count("\n") + 1
            
            fact = JobDependencyFact(
                source_file=source_file,
                line_number=line_number,
                confidence=0.80,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                dependency_job=sql_file,
                dependency_type="hive-file"
            )
            facts.append(fact)
        
        return facts
    
    def _match_to_fact(self, match: dict, source_file: str) -> Optional[Fact]:
        """Convert rule match to fact."""
        from lineage.rules import RuleAction
        
        action = match["action"]
        
        if action == RuleAction.READ_HDFS_PATH:
            path = match["groups"].get("source", match["groups"].get("path", ""))
            return ReadFact(
                source_file=source_file,
                dataset_urn=path,
                dataset_type="hdfs",
                confidence=match["confidence"],
                extraction_method=ExtractionMethod.REGEX,
                evidence=match["match_text"],
                has_placeholders="$" in path
            )
        
        elif action == RuleAction.WRITE_HDFS_PATH:
            path = match["groups"].get("target", match["groups"].get("path", ""))
            return WriteFact(
                source_file=source_file,
                dataset_urn=path,
                dataset_type="hdfs",
                confidence=match["confidence"],
                extraction_method=ExtractionMethod.REGEX,
                evidence=match["match_text"],
                has_placeholders="$" in path
            )
        
        elif action == RuleAction.JOB_INVOCATION:
            script = match["groups"].get("script", match["groups"].get("file", ""))
            fact = JobDependencyFact(
                source_file=source_file,
                confidence=match["confidence"],
                extraction_method=ExtractionMethod.REGEX,
                evidence=match["match_text"],
                dependency_job=script
            )
            return fact
        
        return None
    
    def get_confidence_base(self) -> float:
        """Get base confidence for shell extraction."""
        return 0.70

