"""
Comprehensive tests for NiFi flow extraction.
"""

import pytest
import json
from pathlib import Path
from lineage.extractors.nifi_extractor import NiFiExtractor
from lineage.ir import FactType


class TestNiFiHDFSExtraction:
    """Test HDFS processor extraction."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
    
    def test_get_hdfs_basic(self):
        """Test basic GetHDFS processor extraction."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Get from HDFS",
                    "type": "GetHDFS",
                    "config": {
                        "properties": {
                            "HDFS Directory": "/data/raw/transactions"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        assert len(reads) == 1
        assert reads[0].dataset_urn == "/data/raw/transactions"
        assert reads[0].dataset_type == "hdfs"
        assert reads[0].confidence == 0.75
    
    def test_put_hdfs_basic(self):
        """Test basic PutHDFS processor extraction."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Put to HDFS",
                    "type": "PutHDFS",
                    "config": {
                        "properties": {
                            "Directory": "/data/processed/output"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "/data/processed/output"
        assert writes[0].dataset_type == "hdfs"
    
    def test_hdfs_with_placeholders(self):
        """Test HDFS paths with NiFi placeholders."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Dynamic HDFS",
                    "type": "PutHDFS",
                    "config": {
                        "properties": {
                            "Directory": "/data/output/${processing_date}/${partition}"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert "${processing_date}" in writes[0].dataset_urn
        assert writes[0].has_placeholders == True


class TestNiFiKafkaExtraction:
    """Test Kafka processor extraction."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
    
    def test_consume_kafka(self):
        """Test ConsumeKafka processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Consume Events",
                    "type": "ConsumeKafka_2_6",
                    "config": {
                        "properties": {
                            "topic": "user_events",
                            "Kafka Brokers": "kafka:9092"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        assert len(reads) == 1
        assert reads[0].dataset_urn == "kafka://user_events"
        assert reads[0].dataset_type == "kafka"
    
    def test_publish_kafka(self):
        """Test PublishKafka processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Publish Results",
                    "type": "PublishKafka_2_0",
                    "config": {
                        "properties": {
                            "topic": "processed_events"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "kafka://processed_events"
        assert writes[0].dataset_type == "kafka"
    
    def test_kafka_multiple_topics(self):
        """Test Kafka with multiple topics."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Consume Multiple",
                    "type": "ConsumeKafka_2_6",
                    "config": {
                        "properties": {
                            "Topic Names": "topic1,topic2,topic3"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        assert len(reads) == 1
        assert "topic1,topic2,topic3" in reads[0].dataset_urn


class TestNiFiHiveExtraction:
    """Test Hive processor extraction."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
    
    def test_put_hive_streaming(self):
        """Test PutHive3Streaming processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Stream to Hive",
                    "type": "PutHive3Streaming",
                    "config": {
                        "properties": {
                            "hive-table-name": "prod.transactions"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "prod.transactions"
        assert writes[0].dataset_type == "hive"
    
    def test_put_hiveql_insert(self):
        """Test PutHiveQL with INSERT statement."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Insert to Hive",
                    "type": "PutHiveQL",
                    "config": {
                        "properties": {
                            "SQL Statement": "INSERT INTO analytics.user_events SELECT * FROM staging.temp_events"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "analytics.user_events"
        assert writes[0].dataset_type == "hive"
    
    def test_select_hiveql(self):
        """Test SelectHiveQL for reading."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Query Hive",
                    "type": "SelectHiveQL",
                    "config": {
                        "properties": {
                            "hiveql-query": "SELECT * FROM raw.transactions WHERE date = '2024-01-01'"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        assert len(reads) == 1
        assert reads[0].dataset_urn == "raw.transactions"
        assert reads[0].dataset_type == "hive"
    
    def test_hive_default_database(self):
        """Test Hive with no database specified (should use default)."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Write to Hive",
                    "type": "PutHive3Streaming",
                    "config": {
                        "properties": {
                            "Table Name": "events"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "default.events"


class TestNiFiDatabaseExtraction:
    """Test database processor extraction."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
    
    def test_query_database_table(self):
        """Test QueryDatabaseTable processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Query Customers",
                    "type": "QueryDatabaseTable",
                    "config": {
                        "properties": {
                            "Table Name": "customers.master"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        assert len(reads) == 1
        assert reads[0].dataset_urn == "jdbc://customers.master"
        assert reads[0].dataset_type == "jdbc"
    
    def test_execute_sql_with_query(self):
        """Test ExecuteSQL with SQL query."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Execute Query",
                    "type": "ExecuteSQL",
                    "config": {
                        "properties": {
                            "SQL select query": "SELECT * FROM ecommerce.orders WHERE status = 'completed'"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        assert len(reads) == 1
        assert "ecommerce.orders" in reads[0].dataset_urn
    
    def test_put_sql(self):
        """Test PutSQL processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Write to Database",
                    "type": "PutSQL",
                    "config": {
                        "properties": {
                            "Table Name": "analytics.processed_data"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "jdbc://analytics.processed_data"


class TestNiFiSFTPExtraction:
    """Test SFTP processor extraction."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
    
    def test_get_sftp(self):
        """Test GetSFTP processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Get from SFTP",
                    "type": "GetSFTP",
                    "config": {
                        "properties": {
                            "Hostname": "sftp.vendor.com",
                            "Remote Path": "/exports/daily/data.csv"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        assert len(reads) == 1
        assert reads[0].dataset_urn == "sftp://sftp.vendor.com/exports/daily/data.csv"
        assert reads[0].dataset_type == "sftp"
    
    def test_put_sftp(self):
        """Test PutSFTP processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Put to SFTP",
                    "type": "PutSFTP",
                    "config": {
                        "properties": {
                            "Hostname": "partner.sftp.com",
                            "Remote Path": "/incoming/processed/"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "sftp://partner.sftp.com/incoming/processed/"


class TestNiFiNoSQLExtraction:
    """Test NoSQL processor extraction."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
    
    def test_put_hbase(self):
        """Test PutHBase processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Write to HBase",
                    "type": "PutHBaseJSON",
                    "config": {
                        "properties": {
                            "HBase Table Name": "user_profiles"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "hbase://user_profiles"
        assert writes[0].dataset_type == "hbase"
    
    def test_put_mongo(self):
        """Test PutMongo processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Write to MongoDB",
                    "type": "PutMongo",
                    "config": {
                        "properties": {
                            "Mongo Database Name": "analytics",
                            "Mongo Collection": "events"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "mongodb://analytics/events"
        assert writes[0].dataset_type == "mongodb"
    
    def test_put_elasticsearch(self):
        """Test PutElasticsearch processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Index in Elasticsearch",
                    "type": "PutElasticsearchRecord",
                    "config": {
                        "properties": {
                            "Index": "logs-2024-01"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "elasticsearch://logs-2024-01"
        assert writes[0].dataset_type == "elasticsearch"


class TestNiFiS3Extraction:
    """Test S3 processor extraction."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
    
    def test_fetch_s3(self):
        """Test FetchS3Object processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Fetch from S3",
                    "type": "FetchS3Object",
                    "config": {
                        "properties": {
                            "Bucket": "company-data-lake",
                            "Object Key": "raw/transactions/2024/01/data.parquet"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        assert len(reads) == 1
        assert reads[0].dataset_urn == "s3://company-data-lake/raw/transactions/2024/01/data.parquet"
        assert reads[0].dataset_type == "s3"
    
    def test_put_s3(self):
        """Test PutS3Object processor."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Put to S3",
                    "type": "PutS3Object",
                    "config": {
                        "properties": {
                            "Bucket": "archive-bucket",
                            "Object Key": "processed/${year}/${month}/"
                        }
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(writes) == 1
        assert writes[0].dataset_urn == "s3://archive-bucket/processed/${year}/${month}/"
        assert writes[0].has_placeholders == True


class TestNiFiParameterContexts:
    """Test parameter context extraction."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
    
    def test_parameter_context_extraction(self):
        """Test extraction of parameter contexts."""
        content = json.dumps({
            "parameterContexts": [{
                "name": "ETL-Params",
                "parameters": [
                    {"name": "source_path", "value": "/data/raw"},
                    {"name": "target_table", "value": "prod.processed"}
                ]
            }],
            "flowContents": {"processors": []}
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        configs = [f for f in facts if f.fact_type == FactType.CONFIG]
        assert len(configs) == 2
        
        config_dict = {f.config_key: f.config_value for f in configs}
        assert config_dict["source_path"] == "/data/raw"
        assert config_dict["target_table"] == "prod.processed"


class TestNiFiProcessGroups:
    """Test nested process group extraction."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
    
    def test_single_process_group(self):
        """Test extraction from a process group."""
        content = json.dumps({
            "flowContents": {
                "processors": [],
                "processGroups": [{
                    "identifier": "group-1",
                    "name": "Data Processing",
                    "flowContents": {
                        "processors": [{
                            "identifier": "proc-1",
                            "name": "Get HDFS",
                            "type": "GetHDFS",
                            "config": {
                                "properties": {
                                    "HDFS Directory": "/data/group/input"
                                }
                            }
                        }]
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        assert len(reads) == 1
        assert reads[0].dataset_urn == "/data/group/input"
    
    def test_nested_process_groups(self):
        """Test extraction from nested process groups."""
        content = json.dumps({
            "flowContents": {
                "processors": [],
                "processGroups": [{
                    "identifier": "group-1",
                    "flowContents": {
                        "processors": [{
                            "identifier": "proc-1",
                            "type": "GetHDFS",
                            "config": {"properties": {"HDFS Directory": "/data/level1"}}
                        }],
                        "processGroups": [{
                            "identifier": "nested-group",
                            "flowContents": {
                                "processors": [{
                                    "identifier": "proc-2",
                                    "type": "PutHDFS",
                                    "config": {"properties": {"Directory": "/data/level2"}}
                                }]
                            }
                        }]
                    }
                }]
            }
        })
        
        facts = self.extractor.extract_from_content(content, "test_flow.json")
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        
        assert len(reads) == 1
        assert len(writes) == 1
        assert reads[0].dataset_urn == "/data/level1"
        assert writes[0].dataset_urn == "/data/level2"


class TestNiFiMockFiles:
    """Test extraction from actual mock files."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
        self.mock_dir = Path("tests/mocks/nifi")
    
    def test_hdfs_file_pipeline(self):
        """Test 01_hdfs_file_pipeline.json."""
        file_path = self.mock_dir / "01_hdfs_file_pipeline.json"
        facts = self.extractor.extract(file_path)
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        
        assert len(reads) == 1
        assert len(writes) == 1
        assert "/data/raw/transactions" in reads[0].dataset_urn
        assert "/data/processed/transactions" in writes[0].dataset_urn
    
    def test_kafka_hive_streaming(self):
        """Test 02_kafka_hive_streaming.json."""
        file_path = self.mock_dir / "02_kafka_hive_streaming.json"
        facts = self.extractor.extract(file_path)
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        
        # Should have Kafka read and Hive writes
        kafka_reads = [f for f in reads if f.dataset_type == "kafka"]
        hive_writes = [f for f in writes if f.dataset_type == "hive"]
        
        assert len(kafka_reads) >= 1
        assert len(hive_writes) >= 2
    
    def test_multi_source_ingestion(self):
        """Test 03_multi_source_ingestion.json."""
        file_path = self.mock_dir / "03_multi_source_ingestion.json"
        facts = self.extractor.extract(file_path)
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        
        # Should have SFTP, JDBC, and S3 reads
        sftp_reads = [f for f in reads if f.dataset_type == "sftp"]
        jdbc_reads = [f for f in reads if f.dataset_type == "jdbc"]
        s3_reads = [f for f in reads if f.dataset_type == "s3"]
        
        assert len(sftp_reads) >= 1
        assert len(jdbc_reads) >= 1
        assert len(s3_reads) >= 1
        assert len(writes) >= 1
    
    def test_database_to_nosql(self):
        """Test 04_database_to_nosql.json."""
        file_path = self.mock_dir / "04_database_to_nosql.json"
        facts = self.extractor.extract(file_path)
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        
        # Should have JDBC read and NoSQL writes
        jdbc_reads = [f for f in reads if f.dataset_type == "jdbc"]
        hbase_writes = [f for f in writes if f.dataset_type == "hbase"]
        mongo_writes = [f for f in writes if f.dataset_type == "mongodb"]
        elastic_writes = [f for f in writes if f.dataset_type == "elasticsearch"]
        
        assert len(jdbc_reads) >= 1
        assert len(hbase_writes) >= 1
        assert len(mongo_writes) >= 1
        assert len(elastic_writes) >= 1
    
    def test_parameterized_etl(self):
        """Test 05_parameterized_etl.json."""
        file_path = self.mock_dir / "05_parameterized_etl.json"
        facts = self.extractor.extract(file_path)
        
        configs = [f for f in facts if f.fact_type == FactType.CONFIG]
        reads = [f for f in facts if f.fact_type == FactType.READ]
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        
        # Should have parameter contexts
        assert len(configs) >= 4
        assert len(reads) >= 1
        assert len(writes) >= 2
        
        # Check parameter placeholders
        placeholder_facts = [f for f in reads + writes if f.has_placeholders]
        assert len(placeholder_facts) >= 1
    
    def test_nested_process_groups(self):
        """Test 06_nested_process_groups.json."""
        file_path = self.mock_dir / "06_nested_process_groups.json"
        facts = self.extractor.extract(file_path)
        
        reads = [f for f in facts if f.fact_type == FactType.READ]
        writes = [f for f in facts if f.fact_type == FactType.WRITE]
        
        # Should extract from all levels of nesting
        assert len(reads) >= 2  # Main + group processors
        assert len(writes) >= 2  # Group + nested processors


class TestNiFiEdgeCases:
    """Test edge cases and error handling."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = NiFiExtractor()
    
    def test_empty_flow(self):
        """Test empty flow."""
        content = json.dumps({"flowContents": {"processors": []}})
        facts = self.extractor.extract_from_content(content, "test.json")
        assert facts == []
    
    def test_processor_without_properties(self):
        """Test processor without properties."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Test",
                    "type": "GetHDFS",
                    "config": {}
                }]
            }
        })
        facts = self.extractor.extract_from_content(content, "test.json")
        # Should not crash, may return empty
        assert isinstance(facts, list)
    
    def test_invalid_json(self):
        """Test invalid JSON."""
        content = "{ invalid json }"
        facts = self.extractor.extract_from_content(content, "test.json")
        assert facts == []
    
    def test_processor_not_in_known_types(self):
        """Test processor type not in READ or WRITE lists."""
        content = json.dumps({
            "flowContents": {
                "processors": [{
                    "identifier": "proc-1",
                    "name": "Transform",
                    "type": "UpdateAttribute",
                    "config": {"properties": {}}
                }]
            }
        })
        facts = self.extractor.extract_from_content(content, "test.json")
        # Should not extract anything
        assert len(facts) == 0

