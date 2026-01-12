"""File crawler for repository scanning."""

from pathlib import Path
from typing import List, Set, Iterator, Optional
import fnmatch
from dataclasses import dataclass

from .type_detector import TypeDetector, FileType


@dataclass
class CrawledFile:
    """Information about a crawled file."""
    
    path: Path
    file_type: FileType
    relative_path: Path
    size_bytes: int


class FileCrawler:
    """Crawls repositories and identifies files for extraction."""
    
    def __init__(
        self,
        repo_path: Path,
        ignore_patterns: Optional[List[str]] = None,
        ignore_extensions: Optional[List[str]] = None
    ) -> None:
        """Initialize the file crawler.
        
        Args:
            repo_path: Root path of repository to crawl
            ignore_patterns: List of glob patterns to ignore
            ignore_extensions: List of file extensions to ignore
        """
        self.repo_path = Path(repo_path)
        self.ignore_patterns = ignore_patterns or []
        self.ignore_extensions = ignore_extensions or []
        self.type_detector = TypeDetector()
        
        # Add common ignore patterns if not specified
        if not self.ignore_patterns:
            self.ignore_patterns = [
                "**/.git/**",
                "**/target/**",
                "**/build/**",
                "**/__pycache__/**",
                "**/*.pyc",
                "**/.idea/**",
                "**/.vscode/**",
            ]
    
    def _should_ignore(self, file_path: Path) -> bool:
        """Check if file should be ignored based on patterns."""
        # Check extensions
        if file_path.suffix in self.ignore_extensions:
            return True
        
        # Check glob patterns
        relative_path = file_path.relative_to(self.repo_path)
        path_str = str(relative_path)
        
        for pattern in self.ignore_patterns:
            # Normalize pattern
            if not pattern.startswith("**/"):
                pattern = f"**/{pattern}"
            
            if fnmatch.fnmatch(path_str, pattern):
                return True
        
        return False
    
    def _is_text_file(self, file_path: Path, max_check: int = 8192) -> bool:
        """Check if file is likely a text file."""
        try:
            with open(file_path, "rb") as f:
                chunk = f.read(max_check)
            
            # Check for null bytes (binary indicator)
            if b"\x00" in chunk:
                return False
            
            # Try to decode as UTF-8
            try:
                chunk.decode("utf-8")
                return True
            except UnicodeDecodeError:
                return False
        
        except (IOError, PermissionError):
            return False
    
    def crawl(self) -> Iterator[CrawledFile]:
        """Crawl the repository and yield crawled files."""
        if not self.repo_path.exists():
            raise ValueError(f"Repository path does not exist: {self.repo_path}")
        
        if not self.repo_path.is_dir():
            raise ValueError(f"Repository path is not a directory: {self.repo_path}")
        
        for file_path in self.repo_path.rglob("*"):
            # Skip directories
            if not file_path.is_file():
                continue
            
            # Skip ignored files
            if self._should_ignore(file_path):
                continue
            
            # Skip binary files
            if not self._is_text_file(file_path):
                continue
            
            # Detect file type
            file_type = self.type_detector.detect_file_type(file_path)
            
            # Only yield files we can extract
            if file_type == FileType.UNKNOWN:
                continue
            
            yield CrawledFile(
                path=file_path,
                file_type=file_type,
                relative_path=file_path.relative_to(self.repo_path),
                size_bytes=file_path.stat().st_size
            )
    
    def crawl_by_type(self, file_type: FileType) -> Iterator[CrawledFile]:
        """Crawl and yield only files of a specific type."""
        for crawled_file in self.crawl():
            if crawled_file.file_type == file_type:
                yield crawled_file
    
    def get_all_files(self) -> List[CrawledFile]:
        """Get list of all crawlable files."""
        return list(self.crawl())
    
    def get_files_by_type(self) -> dict[FileType, List[CrawledFile]]:
        """Get files grouped by type."""
        files_by_type: dict[FileType, List[CrawledFile]] = {}
        
        for crawled_file in self.crawl():
            if crawled_file.file_type not in files_by_type:
                files_by_type[crawled_file.file_type] = []
            files_by_type[crawled_file.file_type].append(crawled_file)
        
        return files_by_type
    
    def get_stats(self) -> dict:
        """Get crawling statistics."""
        files_by_type = self.get_files_by_type()
        
        return {
            "total_files": sum(len(files) for files in files_by_type.values()),
            "by_type": {
                file_type.value: len(files)
                for file_type, files in files_by_type.items()
            },
            "total_size_mb": sum(
                f.size_bytes for files in files_by_type.values() for f in files
            ) / (1024 * 1024)
        }

