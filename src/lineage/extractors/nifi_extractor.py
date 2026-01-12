"""NiFi flow.json parser and extractor."""

from pathlib import Path
from typing import List, Optional, Dict, Any
import json

from .base import BaseExtractor
from lineage.ir import Fact, ReadFact, WriteFact, ExtractionMethod


class NiFiExtractor(BaseExtractor):
    """Extractor for NiFi flow definitions."""
    
    # Processor types that read data
    READ_PROCESSORS = {
        "GetHDFS", "FetchHDFS", "GetFile", "FetchFile",
        "GetHive", "SelectHiveQL", "GetHBase", "GetMongo",
        "QueryDatabaseTable", "ExecuteSQL", "ListHDFS", "ListFile"
    }
    
    # Processor types that write data
    WRITE_PROCESSORS = {
        "PutHDFS", "PutFile", "PutHive", "PutHiveQL",
        "PutHBase", "PutMongo", "PutSQL", "PutDatabaseRecord"
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
        # Extract path/table based on processor type
        dataset_urn = None
        dataset_type = "hdfs"
        
        if "HDFS" in proc_type:
            dataset_urn = properties.get("Directory", properties.get("HDFS Directory"))
            dataset_type = "hdfs"
        elif "File" in proc_type:
            dataset_urn = properties.get("Input Directory", properties.get("File to Fetch"))
            dataset_type = "local"
        elif "Hive" in proc_type:
            table = properties.get("hive-table", properties.get("Table Name"))
            if table:
                dataset_urn = f"hive://{table}"
                dataset_type = "hive"
        elif "SQL" in proc_type or "Database" in proc_type:
            table = properties.get("Table Name", properties.get("table-name"))
            if table:
                dataset_urn = f"jdbc://{table}"
                dataset_type = "jdbc"
        
        if not dataset_urn:
            return None
        
        fact = ReadFact(
            source_file=source_file,
            line_number=0,  # N/A for JSON
            dataset_urn=dataset_urn,
            dataset_type=dataset_type,
            confidence=0.70,
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
        # Extract path/table based on processor type
        dataset_urn = None
        dataset_type = "hdfs"
        
        if "HDFS" in proc_type:
            dataset_urn = properties.get("Directory", properties.get("HDFS Directory"))
            dataset_type = "hdfs"
        elif "File" in proc_type:
            dataset_urn = properties.get("Directory", properties.get("Output Directory"))
            dataset_type = "local"
        elif "Hive" in proc_type:
            table = properties.get("hive-table", properties.get("Table Name"))
            if table:
                dataset_urn = f"hive://{table}"
                dataset_type = "hive"
        elif "SQL" in proc_type or "Database" in proc_type:
            table = properties.get("Table Name", properties.get("table-name"))
            if table:
                dataset_urn = f"jdbc://{table}"
                dataset_type = "jdbc"
        
        if not dataset_urn:
            return None
        
        fact = WriteFact(
            source_file=source_file,
            line_number=0,
            dataset_urn=dataset_urn,
            dataset_type=dataset_type,
            confidence=0.70,
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
        return 0.70

