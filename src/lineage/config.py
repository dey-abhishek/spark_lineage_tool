"""Configuration management for lineage tool."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any
import yaml
from pydantic import BaseModel, Field, validator


class IgnorePatternsConfig(BaseModel):
    """Configuration for paths to ignore."""
    
    paths: List[str] = Field(
        default_factory=lambda: [
            "/tmp/**",
            "**/_SUCCESS",
            "**/_logs/**",
            "**/_metadata/**",
            "**/checkpoints/**",
            "**/_temporary/**",
            "**/spark-warehouse/**"
        ]
    )
    extensions: List[str] = Field(default_factory=lambda: [".tmp", ".swp"])


class EnvironmentConfig(BaseModel):
    """Environment-specific configuration."""
    
    name: str
    hdfs_namenode: Optional[str] = None
    hive_metastore_uri: Optional[str] = None
    base_dirs: Dict[str, str] = Field(default_factory=dict)


class HiveMetastoreConfig(BaseModel):
    """Hive metastore connection configuration."""
    
    enabled: bool = False
    jdbc_uri: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    database: str = "default"


class DatabaseConfig(BaseModel):
    """Database export configuration."""
    
    enabled: bool = False
    driver: str = "postgresql"  # postgresql, mysql, sqlite
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    schema: str = "lineage"


class ExportConfig(BaseModel):
    """Export configuration."""
    
    json: bool = True
    csv: bool = True
    database: bool = False
    html: bool = True


class PerformanceConfig(BaseModel):
    """Performance optimization settings."""
    
    parallel_workers: int = 4
    enable_caching: bool = True
    cache_dir: str = ".lineage_cache"
    incremental: bool = True


class LineageConfig(BaseModel):
    """Main lineage tool configuration."""
    
    repo_path: Optional[str] = None
    config_dir: Optional[str] = None
    output_dir: str = "output"
    
    ignore_patterns: IgnorePatternsConfig = Field(default_factory=IgnorePatternsConfig)
    environments: Dict[str, EnvironmentConfig] = Field(default_factory=dict)
    active_environment: str = "default"
    
    hive_metastore: HiveMetastoreConfig = Field(default_factory=HiveMetastoreConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    export: ExportConfig = Field(default_factory=ExportConfig)
    performance: PerformanceConfig = Field(default_factory=PerformanceConfig)
    
    rule_files: List[str] = Field(default_factory=list)
    custom_extractors: Dict[str, str] = Field(default_factory=dict)
    
    @classmethod
    def load_from_file(cls, config_path: Path) -> "LineageConfig":
        """Load configuration from YAML file."""
        with open(config_path, "r") as f:
            data = yaml.safe_load(f)
        return cls(**data)
    
    @classmethod
    def load_default(cls) -> "LineageConfig":
        """Load default configuration."""
        return cls()
    
    def save_to_file(self, config_path: Path) -> None:
        """Save configuration to YAML file."""
        with open(config_path, "w") as f:
            yaml.dump(self.model_dump(), f, default_flow_style=False)
    
    def get_active_environment(self) -> Optional[EnvironmentConfig]:
        """Get the currently active environment configuration."""
        return self.environments.get(self.active_environment)


def load_config(config_path: Optional[str] = None) -> LineageConfig:
    """Load configuration from file or defaults."""
    if config_path:
        return LineageConfig.load_from_file(Path(config_path))
    
    # Try to find config in standard locations
    standard_paths = [
        Path("lineage_config.yaml"),
        Path("config/default_config.yaml"),
        Path.home() / ".lineage" / "config.yaml"
    ]
    
    for path in standard_paths:
        if path.exists():
            return LineageConfig.load_from_file(path)
    
    return LineageConfig.load_default()

