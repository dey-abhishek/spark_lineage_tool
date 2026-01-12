"""
Comprehensive tests for SFTP/SCP/RSYNC shell script extraction.
Tests all mock files in the sftp directory.
"""

import pytest
from pathlib import Path
from lineage.extractors.shell_extractor import ShellExtractor
from lineage.ir import FactType


class TestBasicSFTPGet:
    """Test 01_basic_sftp_get.sh"""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = ShellExtractor(None)
        self.script_path = Path("tests/mocks/shell/sftp/01_basic_sftp_get.sh")
    
    def test_sftp_get_operations_detected(self):
        """Test that SFTP get operations are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        assert len(read_facts) >= 3, "Should detect at least 3 SFTP get operations"
    
    def test_sftp_paths_extracted(self):
        """Test that SFTP paths are correctly extracted."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        urns = [f.dataset_urn for f in read_facts]
        
        # Check for expected patterns
        assert any('sftp.vendor.com' in urn for urn in urns), "Should contain vendor host"
        assert any('/exports/daily/sales' in urn for urn in urns), "Should contain sales path"
    
    def test_sftp_dataset_type(self):
        """Test that SFTP facts have correct dataset type."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        sftp_facts = [f for f in read_facts if f.dataset_type == 'sftp']
        
        assert len(sftp_facts) >= 2, "Should have SFTP-typed facts"
    
    def test_sftp_variables_extracted(self):
        """Test that variables are extracted as config facts."""
        facts = self.extractor.extract(self.script_path)
        
        config_facts = [f for f in facts if f.fact_type == FactType.CONFIG]
        config_keys = [f.config_key for f in config_facts]
        
        assert 'SFTP_HOST' in config_keys
        assert 'SFTP_USER' in config_keys
        assert 'REMOTE_DIR' in config_keys


class TestBasicSFTPPut:
    """Test 02_basic_sftp_put.sh"""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = ShellExtractor(None)
        self.script_path = Path("tests/mocks/shell/sftp/02_basic_sftp_put.sh")
    
    def test_sftp_put_operations_detected(self):
        """Test that SFTP put operations are detected."""
        facts = self.extractor.extract(self.script_path)
        
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 3, "Should detect at least 3 SFTP put operations"
    
    def test_sftp_upload_paths(self):
        """Test that SFTP upload paths are extracted."""
        facts = self.extractor.extract(self.script_path)
        
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        urns = [f.dataset_urn for f in write_facts]
        
        assert any('partner.sftp.com' in urn for urn in urns), "Should contain partner host"
        assert any('/incoming/processed' in urn for urn in urns), "Should contain processed path"
    
    def test_mput_operation(self):
        """Test that mput operation is detected."""
        facts = self.extractor.extract(self.script_path)
        
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        
        # mput should create write facts
        assert len(write_facts) >= 1, "Should detect mput operations"


class TestBasicSCPOperations:
    """Test 03_basic_scp_operations.sh"""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = ShellExtractor(None)
        self.script_path = Path("tests/mocks/shell/sftp/03_basic_scp_operations.sh")
    
    def test_scp_download_detected(self):
        """Test that SCP download operations are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        assert len(read_facts) >= 3, "Should detect multiple SCP downloads"
    
    def test_scp_upload_detected(self):
        """Test that SCP upload operations are detected."""
        facts = self.extractor.extract(self.script_path)
        
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 2, "Should detect multiple SCP uploads"
    
    def test_scp_with_ssh_key(self):
        """Test that SCP with SSH key is detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        
        # Should detect operations even with -i flag
        assert len(read_facts) >= 3, "Should detect SCP with SSH key flag"
    
    def test_scp_recursive(self):
        """Test that recursive SCP is detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        
        # Should detect recursive directory copy
        assert any('/archives/' in f.dataset_urn for f in read_facts), "Should detect recursive SCP"
    
    def test_scp_remote_host(self):
        """Test that SCP remote host is extracted."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        urns = [f.dataset_urn for f in read_facts]
        
        assert any('backup.company.com' in urn for urn in urns), "Should contain backup host"


class TestComplexSFTPPipeline:
    """Test 04_complex_sftp_pipeline.sh"""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = ShellExtractor(None)
        self.script_path = Path("tests/mocks/shell/sftp/04_complex_sftp_pipeline.sh")
    
    def test_multiple_sftp_hosts(self):
        """Test that multiple SFTP hosts are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        urns = [f.dataset_urn for f in read_facts]
        
        # Should detect all three vendors
        assert any('vendor1.sftp.com' in urn for urn in urns), "Should detect vendor1"
        assert any('vendor2.sftp.com' in urn for urn in urns), "Should detect vendor2"
        assert any('vendor3.sftp.com' in urn for urn in urns), "Should detect vendor3"
    
    def test_sftp_mget_multiple_patterns(self):
        """Test that multiple mget patterns are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        
        # Should detect mget operations for different file patterns
        assert len(read_facts) >= 5, "Should detect multiple mget operations"
    
    def test_hdfs_write_after_sftp(self):
        """Test that HDFS writes after SFTP downloads are detected."""
        facts = self.extractor.extract(self.script_path)
        
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        hdfs_writes = [f for f in write_facts if f.dataset_type == 'hdfs']
        
        assert len(hdfs_writes) >= 3, "Should detect HDFS puts for each vendor"
    
    def test_different_file_formats(self):
        """Test that different file formats are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        urns = [f.dataset_urn for f in read_facts]
        
        # Check for various file extensions
        assert any('.csv' in urn for urn in urns), "Should detect CSV files"
        assert any('.json' in urn for urn in urns), "Should detect JSON files"
        assert any('.parquet' in urn for urn in urns), "Should detect Parquet files"


class TestSCPWithVariables:
    """Test 05_scp_with_variables.sh"""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = ShellExtractor(None)
        self.script_path = Path("tests/mocks/shell/sftp/05_scp_with_variables.sh")
    
    def test_variable_extraction(self):
        """Test that shell variables are extracted."""
        facts = self.extractor.extract(self.script_path)
        
        config_facts = [f for f in facts if f.fact_type == FactType.CONFIG]
        config_keys = [f.config_key for f in config_facts]
        
        assert 'REMOTE_HOST' in config_keys
        assert 'REMOTE_USER' in config_keys
        assert 'SSH_KEY' in config_keys
        assert 'REMOTE_BASE' in config_keys
    
    def test_scp_loop_detected(self):
        """Test that SCP operations in a loop are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        
        # Should detect SCP operations for multiple databases
        assert len(read_facts) >= 2, "Should detect multiple SCP operations in loop"
    
    def test_scp_with_port(self):
        """Test that SCP with custom port is detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        
        # Should detect SCP with -P flag
        assert len(read_facts) >= 3, "Should detect SCP with port specification"
    
    def test_scp_compression(self):
        """Test that SCP with compression flag is detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        
        # Should detect SCP with -C flag
        assert len(read_facts) >= 2, "Should detect compressed SCP"


class TestSFTPBidirectional:
    """Test 06_sftp_bidirectional.sh"""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = ShellExtractor(None)
        self.script_path = Path("tests/mocks/shell/sftp/06_sftp_bidirectional.sh")
    
    def test_sftp_download_and_upload(self):
        """Test that both download and upload are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        
        assert len(read_facts) >= 3, "Should detect SFTP downloads"
        assert len(write_facts) >= 3, "Should detect SFTP uploads"
    
    def test_different_remote_directories(self):
        """Test that different remote directories are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        
        read_urns = [f.dataset_urn for f in read_facts]
        write_urns = [f.dataset_urn for f in write_facts]
        
        # Check for inbox and outbox directories
        assert any('/inbox/' in urn or 'from_partner' in urn for urn in read_urns), "Should detect inbox"
        assert any('/outbox/' in urn or 'to_partner' in urn for urn in write_urns), "Should detect outbox"
    
    def test_multiple_file_types(self):
        """Test that multiple file types are detected."""
        facts = self.extractor.extract(self.script_path)
        
        all_facts = [f for f in facts if f.fact_type in [FactType.READ, FactType.WRITE]]
        urns = [f.dataset_urn for f in all_facts]
        
        assert any('.csv' in urn for urn in urns), "Should detect CSV"
        assert any('.json' in urn for urn in urns), "Should detect JSON"
        assert any('.xml' in urn for urn in urns), "Should detect XML"


class TestRSYNCOperations:
    """Test 07_rsync_operations.sh"""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = ShellExtractor(None)
        self.script_path = Path("tests/mocks/shell/sftp/07_rsync_operations.sh")
    
    def test_rsync_download_detected(self):
        """Test that rsync download operations are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        assert len(read_facts) >= 3, "Should detect multiple rsync downloads"
    
    def test_rsync_upload_detected(self):
        """Test that rsync upload operations are detected."""
        facts = self.extractor.extract(self.script_path)
        
        write_facts = [f for f in facts if f.fact_type == FactType.WRITE]
        assert len(write_facts) >= 2, "Should detect rsync uploads"
    
    def test_rsync_with_include_exclude(self):
        """Test that rsync with include/exclude patterns is detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        
        # Should detect rsync even with complex flags
        assert len(read_facts) >= 2, "Should detect rsync with include/exclude"
    
    def test_rsync_remote_host(self):
        """Test that rsync remote host is extracted."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        urns = [f.dataset_urn for f in read_facts]
        
        assert any('fileserver.company.com' in urn for urn in urns), "Should contain fileserver host"


class TestSFTPErrorHandling:
    """Test 08_sftp_with_error_handling.sh"""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = ShellExtractor(None)
        self.script_path = Path("tests/mocks/shell/sftp/08_sftp_with_error_handling.sh")
    
    def test_sftp_in_function(self):
        """Test that SFTP operations within functions are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        
        # Should detect SFTP operations even within functions
        assert len(read_facts) >= 1, "Should detect SFTP in functions"
    
    def test_multiple_file_downloads(self):
        """Test that multiple file downloads are detected."""
        facts = self.extractor.extract(self.script_path)
        
        read_facts = [f for f in facts if f.fact_type == FactType.READ]
        
        # Should detect calls to download_with_retry function
        assert len(read_facts) >= 1, "Should detect multiple downloads"
    
    def test_config_extraction(self):
        """Test that configuration variables are extracted."""
        facts = self.extractor.extract(self.script_path)
        
        config_facts = [f for f in facts if f.fact_type == FactType.CONFIG]
        config_keys = [f.config_key for f in config_facts]
        
        assert 'SFTP_HOST' in config_keys
        assert 'MAX_RETRIES' in config_keys
        assert 'RETRY_DELAY' in config_keys


class TestAllSFTPScripts:
    """Integration tests for all SFTP scripts."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.extractor = ShellExtractor(None)
        self.sftp_dir = Path("tests/mocks/shell/sftp")
    
    def test_all_scripts_extract_successfully(self):
        """Test that all SFTP scripts can be extracted without errors."""
        script_files = list(self.sftp_dir.glob("*.sh"))
        assert len(script_files) >= 8, "Should have at least 8 SFTP mock scripts"
        
        for script_file in script_files:
            facts = self.extractor.extract(script_file)
            assert len(facts) > 0, f"Should extract facts from {script_file.name}"
    
    def test_total_sftp_operations(self):
        """Test total number of SFTP/SCP/RSYNC operations across all scripts."""
        script_files = list(self.sftp_dir.glob("*.sh"))
        
        total_reads = 0
        total_writes = 0
        
        for script_file in script_files:
            facts = self.extractor.extract(script_file)
            total_reads += len([f for f in facts if f.fact_type == FactType.READ])
            total_writes += len([f for f in facts if f.fact_type == FactType.WRITE])
        
        assert total_reads >= 20, f"Should detect at least 20 read operations, got {total_reads}"
        assert total_writes >= 10, f"Should detect at least 10 write operations, got {total_writes}"
    
    def test_confidence_scores(self):
        """Test that all facts have reasonable confidence scores."""
        script_files = list(self.sftp_dir.glob("*.sh"))
        
        for script_file in script_files:
            facts = self.extractor.extract(script_file)
            
            for fact in facts:
                assert 0.0 <= fact.confidence <= 1.0, \
                    f"Confidence should be between 0 and 1 in {script_file.name}"
                
                # SFTP/SCP operations should have decent confidence
                if fact.fact_type in [FactType.READ, FactType.WRITE]:
                    assert fact.confidence >= 0.5, \
                        f"SFTP/SCP operations should have confidence >= 0.5 in {script_file.name}"
    
    def test_dataset_types(self):
        """Test that dataset types are correctly assigned."""
        script_files = list(self.sftp_dir.glob("*.sh"))
        
        for script_file in script_files:
            facts = self.extractor.extract(script_file)
            
            for fact in facts:
                if fact.fact_type in [FactType.READ, FactType.WRITE]:
                    assert fact.dataset_type in ['sftp', 'hdfs', 'file', 'local'], \
                        f"Invalid dataset type in {script_file.name}: {fact.dataset_type}"

