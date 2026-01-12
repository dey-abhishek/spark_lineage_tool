"""NiFi flow.json parser and extractor."""

from pathlib import Path
from typing import List, Optional, Dict, Any
import json
import re

from .base import BaseExtractor
from lineage.ir import Fact, ReadFact, WriteFact, ConfigFact, ExtractionMethod


class NiFiExtractor(BaseExtractor):
    """Extractor for NiFi flow definitions."""
    
    # Processor types that read data
    READ_PROCESSORS = {
        # HDFS
        "GetHDFS", "FetchHDFS", "ListHDFS",
        # File
        "GetFile", "FetchFile", "ListFile", "TailFile",
        # SFTP/FTP
        "GetSFTP", "FetchSFTP", "ListSFTP", "GetFTP", "FetchFTP",
        # Kafka
        "ConsumeKafka", "ConsumeKafka_2_0", "ConsumeKafka_2_6", "ConsumeKafkaRecord",
        # Database
        "QueryDatabaseTable", "ExecuteSQL", "GenerateTableFetch", "ListDatabaseTables",
        # Hive
        "GetHive", "SelectHiveQL",
        # HBase
        "GetHBase", "FetchHBase", "ScanHBase",
        # Mongo
        "GetMongo", "GetMongoRecord",
        # S3
        "FetchS3Object", "ListS3",
        # HTTP/REST
        "InvokeHTTP", "GetHTTP",
    }
    
    # Processor types that write data
    WRITE_PROCESSORS = {
        # HDFS
        "PutHDFS",
        # File
        "PutFile",
        # SFTP/FTP
        "PutSFTP", "PutFTP",
        # Kafka
        "PublishKafka", "PublishKafka_2_0", "PublishKafka_2_6", "PublishKafkaRecord",
        # Database
        "PutSQL", "PutDatabaseRecord", "ExecuteSQLRecord",
        # Hive
        "PutHive", "PutHiveQL", "PutHiveStreaming", "PutHive3Streaming",
        # HBase
        "PutHBase", "PutHBaseRecord", "PutHBaseCell", "PutHBaseJSON",
        # Mongo
        "PutMongo", "PutMongoRecord",
        # S3
        "PutS3Object",
        # Elasticsearch
        "PutElasticsearch", "PutElasticsearchHttp", "PutElasticsearchRecord",
    }
    
    def __init__(self) -> None:
        super().__init__()
    
    def extract(self, file_path: Path) -> List[Fact]:
        """Extract facts from NiFi flow file."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                flow_data = json.load(f)
            
            return self.extract_from_content(json.dumps(flow_data), str(file_path))
        
        except Exception as e:
            print(f"Error extracting from {file_path}: {e}")
            return []
    
    def extract_from_content(self, content: str, source_file: str) -> List[Fact]:
        """Extract facts from NiFi flow content."""
        try:
            flow_data = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON in {source_file}: {e}")
            return []
        
        facts = []
        
        # Extract from flow snapshot
        if "flowContents" in flow_data:
            facts.extend(self._extract_from_flow(flow_data["flowContents"], source_file))
        elif "processors" in flow_data:
            facts.extend(self._extract_from_flow(flow_data, source_file))
        
        # Extract parameter contexts if available
        if "parameterContexts" in flow_data:
            facts.extend(self._extract_parameter_contexts(flow_data["parameterContexts"], source_file))
        
        return facts
    
    def _extract_parameter_contexts(self, contexts: List[Dict[str, Any]], source_file: str) -> List[Fact]:
        """Extract parameter contexts as configuration facts."""
        facts = []
        
        for context in contexts:
            context_name = context.get("name", "")
            parameters = context.get("parameters", [])
            
            for param in parameters:
                param_name = param.get("name", "")
                param_value = param.get("value", "")
                
                if param_name and param_value:
                    fact = ConfigFact(
                        source_file=source_file,
                        line_number=0,
                        confidence=0.95,
                        extraction_method=ExtractionMethod.JSON_PARSE,
                        evidence=f"Parameter Context: {context_name}",
                        config_key=param_name,
                        config_value=param_value
                    )
                    facts.append(fact)
        
        return facts
    
    def _extract_from_flow(self, flow: Dict[str, Any], source_file: str) -> List[Fact]:
        """Extract facts from flow contents."""
        facts = []
        
        # Extract from processors
        processors = flow.get("processors", [])
        for processor in processors:
            proc_facts = self._extract_from_processor(processor, source_file)
            facts.extend(proc_facts)
        
        # Extract from process groups (recursive)
        process_groups = flow.get("processGroups", [])
        for group in process_groups:
            if "flowContents" in group or "processors" in group:
                group_flow = group.get("flowContents", group)
                facts.extend(self._extract_from_flow(group_flow, source_file))
        
        return facts
    
    def _extract_from_processor(
        self,
        processor: Dict[str, Any],
        source_file: str
    ) -> List[Fact]:
        """Extract facts from a single processor."""
        facts = []
        
        # Get processor info
        proc_type = processor.get("type", "")
        proc_name = processor.get("name", "")
        proc_id = processor.get("id", "")
        
        # Get processor config
        config = processor.get("config", {})
        properties = config.get("properties", {})
        
        # Determine if read or write processor
        if any(read_type in proc_type for read_type in self.READ_PROCESSORS):
            fact = self._create_read_fact(
                proc_type, proc_name, properties, source_file, proc_id
            )
            if fact:
                facts.append(fact)
        
        if any(write_type in proc_type for write_type in self.WRITE_PROCESSORS):
            fact = self._create_write_fact(
                proc_type, proc_name, properties, source_file, proc_id
            )
            if fact:
                facts.append(fact)
        
        return facts
    
    def _create_read_fact(
        self,
        proc_type: str,
        proc_name: str,
        properties: Dict[str, Any],
        source_file: str,
        proc_id: str
    ) -> Optional[Fact]:
        """Create read fact from processor."""
        dataset_urn = None
        dataset_type = "hdfs"
        
        # HDFS processors
        if "HDFS" in proc_type:
            dataset_urn = properties.get("Directory") or properties.get("HDFS Directory") or properties.get("Directory Path")
            dataset_type = "hdfs"
        
        # File processors
        elif "File" in proc_type and "HDFS" not in proc_type:
            dataset_urn = properties.get("Input Directory") or properties.get("File to Fetch") or properties.get("Directory")
            dataset_type = "file"
        
        # SFTP/FTP processors
        elif "SFTP" in proc_type:
            remote_path = properties.get("Remote Path") or properties.get("Path") or properties.get("Remote File")
            hostname = properties.get("Hostname", "unknown_host")
            if remote_path:
                dataset_urn = f"sftp://{hostname}{remote_path}"
                dataset_type = "sftp"
        
        elif "FTP" in proc_type:
            remote_path = properties.get("Remote Path") or properties.get("Path")
            hostname = properties.get("Hostname", "unknown_host")
            if remote_path:
                dataset_urn = f"ftp://{hostname}{remote_path}"
                dataset_type = "ftp"
        
        # Kafka processors
        elif "Kafka" in proc_type:
            topic = properties.get("topic") or properties.get("Topic Name") or properties.get("Topic Names")
            if topic:
                dataset_urn = f"kafka://{topic}"
                dataset_type = "kafka"
        
        # Hive processors
        elif "Hive" in proc_type:
            table = properties.get("hive-table") or properties.get("Table Name") or properties.get("hive-table-name")
            # Try to extract from HiveQL if table not found
            if not table:
                sql_statement = properties.get("SQL Statement", "") or properties.get("hiveql-query", "")
                if sql_statement:
                    # Extract table from SELECT FROM
                    match = re.search(r'FROM\s+([\w.#{}]+)', sql_statement, re.IGNORECASE)
                    if match:
                        table = match.group(1)
            
            if table:
                # Handle database.table format
                if "." in table:
                    dataset_urn = table
                else:
                    dataset_urn = f"default.{table}"
                dataset_type = "hive"
        
        # HBase processors
        elif "HBase" in proc_type:
            table = properties.get("Table Name") or properties.get("table-name") or properties.get("HBase Table Name")
            if table:
                dataset_urn = f"hbase://{table}"
                dataset_type = "hbase"
        
        # MongoDB processors
        elif "Mongo" in proc_type:
            collection = properties.get("Mongo Collection") or properties.get("Collection Name")
            database = properties.get("Mongo Database Name") or properties.get("Database Name", "default")
            if collection:
                dataset_urn = f"mongodb://{database}/{collection}"
                dataset_type = "mongodb"
        
        # SQL/Database processors
        elif "SQL" in proc_type or "Database" in proc_type:
            table = properties.get("Table Name") or properties.get("table-name")
            # Try to extract from SQL
            if not table:
                sql_statement = properties.get("SQL select query", "") or properties.get("SQL Query", "")
                if sql_statement:
                    match = re.search(r'FROM\s+([\w.#{}]+)', sql_statement, re.IGNORECASE)
                    if match:
                        table = match.group(1)
            
            if table:
                dataset_urn = f"jdbc://{table}"
                dataset_type = "jdbc"
        
        # S3 processors
        elif "S3" in proc_type:
            bucket = properties.get("Bucket") or properties.get("Bucket Name")
            key = properties.get("Object Key") or properties.get("Prefix")
            if bucket:
                dataset_urn = f"s3://{bucket}/{key}" if key else f"s3://{bucket}"
                dataset_type = "s3"
        
        # HTTP/REST processors
        elif "HTTP" in proc_type:
            url = properties.get("Remote URL") or properties.get("HTTP URL")
            if url:
                dataset_urn = url
                dataset_type = "http"
        
        if not dataset_urn:
            return None
        
        fact = ReadFact(
            source_file=source_file,
            line_number=0,  # N/A for JSON
            dataset_urn=dataset_urn,
            dataset_type=dataset_type,
            confidence=0.75,
            extraction_method=ExtractionMethod.JSON_PARSE,
            evidence=f"{proc_type}: {proc_name}",
            has_placeholders="${" in dataset_urn or "#{" in dataset_urn
        )
        
        fact.params["processor_type"] = proc_type
        fact.params["processor_name"] = proc_name
        fact.params["processor_id"] = proc_id
        
        return fact
    
    def _create_write_fact(
        self,
        proc_type: str,
        proc_name: str,
        properties: Dict[str, Any],
        source_file: str,
        proc_id: str
    ) -> Optional[Fact]:
        """Create write fact from processor."""
        dataset_urn = None
        dataset_type = "hdfs"
        
        # HDFS processors
        if "HDFS" in proc_type:
            dataset_urn = properties.get("Directory") or properties.get("HDFS Directory") or properties.get("Directory Path")
            dataset_type = "hdfs"
        
        # File processors
        elif "File" in proc_type and "HDFS" not in proc_type:
            dataset_urn = properties.get("Directory") or properties.get("Output Directory")
            dataset_type = "file"
        
        # SFTP/FTP processors
        elif "SFTP" in proc_type:
            remote_path = properties.get("Remote Path") or properties.get("Path")
            hostname = properties.get("Hostname", "unknown_host")
            if remote_path:
                dataset_urn = f"sftp://{hostname}{remote_path}"
                dataset_type = "sftp"
        
        elif "FTP" in proc_type:
            remote_path = properties.get("Remote Path") or properties.get("Path")
            hostname = properties.get("Hostname", "unknown_host")
            if remote_path:
                dataset_urn = f"ftp://{hostname}{remote_path}"
                dataset_type = "ftp"
        
        # Kafka processors
        elif "Kafka" in proc_type:
            topic = properties.get("topic") or properties.get("Topic Name")
            if topic:
                dataset_urn = f"kafka://{topic}"
                dataset_type = "kafka"
        
        # Hive processors
        elif "Hive" in proc_type or "HiveQL" in proc_type:
            table = properties.get("hive-table") or properties.get("Table Name") or properties.get("hive-table-name")
            # Try to extract from HiveQL if table not found
            if not table:
                sql_statement = properties.get("SQL Statement", "") or properties.get("hiveql-query", "")
                if sql_statement:
                    # Extract table from INSERT INTO
                    match = re.search(r'INSERT\s+(?:INTO|OVERWRITE)\s+(?:TABLE\s+)?([\w.#{}]+)', sql_statement, re.IGNORECASE)
                    if match:
                        table = match.group(1)
            
            if table:
                # Handle database.table format
                if "." in table:
                    dataset_urn = table
                else:
                    dataset_urn = f"default.{table}"
                dataset_type = "hive"
        
        # HBase processors
        elif "HBase" in proc_type:
            table = properties.get("Table Name") or properties.get("table-name") or properties.get("HBase Table Name")
            if table:
                dataset_urn = f"hbase://{table}"
                dataset_type = "hbase"
        
        # MongoDB processors
        elif "Mongo" in proc_type:
            collection = properties.get("Mongo Collection") or properties.get("Collection Name")
            database = properties.get("Mongo Database Name") or properties.get("Database Name", "default")
            if collection:
                dataset_urn = f"mongodb://{database}/{collection}"
                dataset_type = "mongodb"
        
        # SQL/Database processors
        elif "SQL" in proc_type or "Database" in proc_type:
            table = properties.get("Table Name") or properties.get("table-name")
            # Try to extract from SQL
            if not table:
                sql_statement = properties.get("SQL Statement", "")
                if sql_statement:
                    match = re.search(r'INSERT\s+INTO\s+([\w.#{}]+)', sql_statement, re.IGNORECASE)
                    if match:
                        table = match.group(1)
            
            if table:
                dataset_urn = f"jdbc://{table}"
                dataset_type = "jdbc"
        
        # S3 processors
        elif "S3" in proc_type:
            bucket = properties.get("Bucket") or properties.get("Bucket Name")
            key = properties.get("Object Key") or properties.get("Prefix")
            if bucket:
                dataset_urn = f"s3://{bucket}/{key}" if key else f"s3://{bucket}"
                dataset_type = "s3"
        
        # Elasticsearch processors
        elif "Elasticsearch" in proc_type:
            index = properties.get("Index") or properties.get("Index Name") or properties.get("Identifier")
            if index:
                dataset_urn = f"elasticsearch://{index}"
                dataset_type = "elasticsearch"
        
        if not dataset_urn:
            return None
        
        fact = WriteFact(
            source_file=source_file,
            line_number=0,
            dataset_urn=dataset_urn,
            dataset_type=dataset_type,
            confidence=0.75,
            extraction_method=ExtractionMethod.JSON_PARSE,
            evidence=f"{proc_type}: {proc_name}",
            has_placeholders="${" in dataset_urn or "#{" in dataset_urn
        )
        
        fact.params["processor_type"] = proc_type
        fact.params["processor_name"] = proc_name
        fact.params["processor_id"] = proc_id
        
        return fact
    
    def get_confidence_base(self) -> float:
        """Get base confidence for NiFi extraction."""
        return 0.75

