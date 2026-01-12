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
            "distcp": re.compile(r'hadoop\s+distcp(?:\s+|[\s\\]+)*(?:-[\w-]+(?:\s+|[\s\\]+)+)*([^\s\\]+)(?:\s+|[\s\\]+)+([^\s\\]+)', re.MULTILINE),
            "spark_submit": re.compile(r'spark-submit\s+.*?(\S+\.(?:py|jar))(?:[ \t]+([^\n]*))?', re.MULTILINE),
            "spark_submit_full": re.compile(r'spark-submit[ \t]+([^\n]+)', re.MULTILINE),
            "hive_execute": re.compile(r'hive\s+-e\s+["\']([^"\']+)["\']'),
            "hive_file": re.compile(r'hive\s+-f\s+(\S+)'),
            "beeline_execute": re.compile(r'beeline\s+.*?-e\s+["\']([^"\']+)["\']', re.DOTALL),
            "beeline_file": re.compile(r'beeline\s+.*?-f\s+(\S+)'),
            "cron_job": re.compile(r'^[\s#]*([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+(.+)$', re.MULTILINE),
            "cron_special": re.compile(r'^[\s#]*(@(?:yearly|annually|monthly|weekly|daily|hourly|reboot))\s+(.+)$', re.MULTILINE),
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
        
        # Check if this is a crontab file (before removing comments)
        if source_file.endswith(('crontab', '.cron', 'cron.txt')) or 'cron' in source_file.lower():
            facts.extend(self._extract_cron_jobs(content, source_file))
        
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
        
        # spark-submit (enhanced to capture configs and arguments)
        for match in self.patterns["spark_submit_full"].finditer(content):
            full_command_line = match.group(1).strip()  # Everything after "spark-submit"
            full_command = "spark-submit " + full_command_line
            line_number = content[:match.start()].count("\n") + 1
            
            # Find the actual main application file (excluding --jars, --py-files, etc.)
            main_app = self._find_main_application_file(full_command)
            if not main_app:
                # Skip if we can't find the main app
                continue
            
            # Determine job type
            job_type = "pyspark" if main_app.endswith(".py") else "spark-jar"
            
            # Split the command into configs (before main app) and args (after main app)
            if main_app in full_command_line:
                app_idx = full_command_line.index(main_app)
                spark_configs = full_command_line[:app_idx].strip()
                script_args = full_command_line[app_idx + len(main_app):].strip()
            else:
                spark_configs = full_command_line
                script_args = ""
            
            fact = JobDependencyFact(
                source_file=source_file,
                line_number=line_number,
                confidence=0.85,
                extraction_method=ExtractionMethod.REGEX,
                evidence=match.group(0),
                dependency_job=main_app,
                dependency_type="spark-submit"
            )
            
            # Extract spark configurations
            fact.params["job_type"] = job_type
            fact.params["spark_configs"] = self._parse_spark_configs(spark_configs)
            fact.params["script_args"] = self._parse_script_args(script_args)
            
            # Extract key configs for easier access
            configs = fact.params["spark_configs"]
            if "master" in configs:
                fact.params["master"] = configs["master"]
            if "deploy-mode" in configs:
                fact.params["deploy_mode"] = configs["deploy-mode"]
            if "class" in configs:
                fact.params["main_class"] = configs["class"]
            
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
    
    def _parse_spark_configs(self, config_string: str) -> Dict[str, str]:
        """Parse spark-submit configuration flags."""
        configs = {}
        
        # Pattern to match --config "quoted value" or --config 'quoted value' or --config value
        quoted_pattern = re.compile(r'--([a-zA-Z][-a-zA-Z0-9]*)\s+"([^"]*)"')
        single_quoted_pattern = re.compile(r"--([a-zA-Z][-a-zA-Z0-9]*)\s+'([^']*)'")
        equals_quoted_pattern = re.compile(r'--([a-zA-Z][-a-zA-Z0-9]*)="([^"]*)"')
        equals_single_quoted_pattern = re.compile(r"--([a-zA-Z][-a-zA-Z0-9]*)='([^']*)'")
        unquoted_pattern = re.compile(r'--([a-zA-Z][-a-zA-Z0-9]*)\s+([^\s-][^\s]*)')
        equals_pattern = re.compile(r'--([a-zA-Z][-a-zA-Z0-9]*)=([^\s]+)')
        
        # Extract --key "value" patterns (double quotes)
        for match in quoted_pattern.finditer(config_string):
            key = match.group(1)
            value = match.group(2)
            configs[key] = value
        
        # Extract --key 'value' patterns (single quotes)
        for match in single_quoted_pattern.finditer(config_string):
            key = match.group(1)
            value = match.group(2)
            configs[key] = value
        
        # Extract --key="value" patterns (double quotes)
        for match in equals_quoted_pattern.finditer(config_string):
            key = match.group(1)
            value = match.group(2)
            configs[key] = value
        
        # Extract --key='value' patterns (single quotes)
        for match in equals_single_quoted_pattern.finditer(config_string):
            key = match.group(1)
            value = match.group(2)
            configs[key] = value
        
        # Extract --key value patterns (unquoted) - but skip if already captured as quoted
        for match in unquoted_pattern.finditer(config_string):
            key = match.group(1)
            if key not in configs:  # Don't overwrite quoted values
                value = match.group(2).strip('"\'')
                configs[key] = value
        
        # Extract --key=value patterns (unquoted)
        for match in equals_pattern.finditer(config_string):
            key = match.group(1)
            value = match.group(2).strip('"\'')
            if key not in configs:  # Don't overwrite existing
                configs[key] = value
        
        # Extract --conf spark.config=value
        conf_pattern = re.compile(r'--conf\s+([^=\s]+)=([^\s]+)')
        spark_confs = {}
        for match in conf_pattern.finditer(config_string):
            conf_key = match.group(1)
            conf_value = match.group(2).strip('"\'')
            spark_confs[conf_key] = conf_value
        
        if spark_confs:
            configs["spark_confs"] = spark_confs
        
        return configs
    
    def _parse_script_args(self, args_string: str) -> List[str]:
        """Parse script arguments (everything after the .py or .jar file)."""
        if not args_string:
            return []
        
        # Simple split by whitespace, but preserve quoted strings
        args = []
        current_arg = []
        in_quote = None
        
        for char in args_string:
            if char in ('"', "'"):
                if in_quote == char:
                    in_quote = None
                elif in_quote is None:
                    in_quote = char
                else:
                    current_arg.append(char)
            elif char.isspace() and in_quote is None:
                if current_arg:
                    args.append(''.join(current_arg))
                    current_arg = []
            else:
                current_arg.append(char)
        
        if current_arg:
            args.append(''.join(current_arg))
        
        return args
    
    def _extract_cron_jobs(self, content: str, source_file: str) -> List[Fact]:
        """Extract cron job definitions that may contain spark-submit commands."""
        facts = []
        
        # Handle special cron syntax (@daily, @hourly, etc.)
        for match in self.patterns["cron_special"].finditer(content):
            schedule_type, command = match.groups()
            line_number = content[:match.start()].count("\n") + 1
            
            # Skip if it's a comment
            if match.group(0).strip().startswith('#'):
                continue
            
            # Check if the command contains spark-submit
            if 'spark-submit' in command:
                fact = JobDependencyFact(
                    source_file=source_file,
                    line_number=line_number,
                    confidence=0.75,
                    extraction_method=ExtractionMethod.REGEX,
                    evidence=match.group(0),
                    dependency_type="cron-spark-submit"
                )
                
                fact.params["cron_schedule"] = {"schedule_type": schedule_type}
                fact.params["cron_command"] = command.strip()
                
                # Try to extract the spark script using the main app finder
                main_app = self._find_main_application_file("spark-submit " + command)
                if main_app:
                    fact.params["spark_script"] = main_app
                    fact.dependency_job = main_app
                
                facts.append(fact)
        
        # Handle standard cron format
        for match in self.patterns["cron_job"].finditer(content):
            minute, hour, day, month, weekday, command = match.groups()
            line_number = content[:match.start()].count("\n") + 1
            
            # Skip if it's a comment
            if match.group(0).strip().startswith('#'):
                continue
            
            # Check if the command contains spark-submit
            if 'spark-submit' in command:
                fact = JobDependencyFact(
                    source_file=source_file,
                    line_number=line_number,
                    confidence=0.75,
                    extraction_method=ExtractionMethod.REGEX,
                    evidence=match.group(0),
                    dependency_type="cron-spark-submit"
                )
                
                fact.params["cron_schedule"] = {
                    "minute": minute,
                    "hour": hour,
                    "day": day,
                    "month": month,
                    "weekday": weekday
                }
                fact.params["cron_command"] = command.strip()
                
                # Try to extract the spark script using the main app finder
                main_app = self._find_main_application_file("spark-submit " + command)
                if main_app:
                    fact.params["spark_script"] = main_app
                    fact.dependency_job = main_app
                
                facts.append(fact)
            elif any(cmd in command for cmd in ['hive', 'beeline', 'hadoop']):
                # Also capture other big data job schedulers
                fact = JobDependencyFact(
                    source_file=source_file,
                    line_number=line_number,
                    confidence=0.70,
                    extraction_method=ExtractionMethod.REGEX,
                    evidence=match.group(0),
                    dependency_type="cron-job"
                )
                
                fact.params["cron_schedule"] = {
                    "minute": minute,
                    "hour": hour,
                    "day": day,
                    "month": month,
                    "weekday": weekday
                }
                fact.params["cron_command"] = command.strip()
                
                facts.append(fact)
        
        return facts
    
    def _find_main_application_file(self, spark_submit_command: str) -> Optional[str]:
        """
        Find the main application .py or .jar file in a spark-submit command.
        Excludes files that are part of --jars, --py-files, --files, --archives.
        """
        # Find all .jar and .py files in the command
        all_files = re.findall(r'([/\w.-]+\.(?:jar|py))', spark_submit_command)
        
        if not all_files:
            return None
        
        # Find values of --jars, --py-files, --files, --archives (comma-separated lists)
        dependency_files = set()
        
        for flag in ['--jars', '--py-files', '--files', '--archives']:
            match = re.search(rf'{flag}[ \t]+([^\s]+)', spark_submit_command)
            if match:
                value = match.group(1)
                # Split by comma and add all files
                for f in value.split(','):
                    dependency_files.add(f.strip())
        
        # The main application file is one that's NOT in the dependency lists
        for f in all_files:
            if f not in dependency_files:
                return f
        
        # If all files are in dependency lists, return the first one (shouldn't happen)
        return all_files[0] if all_files else None
    
    def get_confidence_base(self) -> float:
        """Get base confidence for shell extraction."""
        return 0.70

