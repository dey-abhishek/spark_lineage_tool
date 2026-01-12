"""CLI for lineage analysis tool."""

import click
from pathlib import Path
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
import sys

from lineage.config import load_config, LineageConfig
from lineage.crawler import FileCrawler, FileType
from lineage.extractors import (
    PySparkExtractor, ScalaExtractor, HiveExtractor,
    ShellExtractor, NiFiExtractor, ConfigExtractor
)
from lineage.ir import FactStore
from lineage.rules import create_default_engine
from lineage.resolution import SymbolTable, PathCanonicalizer, VariableResolver
from lineage.lineage import LineageBuilder
from lineage.scoring import ConfidenceScorer, PriorityCalculator
from lineage.exporters import JSONExporter, CSVExporter, DatabaseExporter, HTMLExporter, ExcelExporter

console = Console()


@click.command()
@click.option("--repo", required=True, help="Path to repository to scan")
@click.option("--config", default=None, help="Path to configuration file")
@click.option("--out", default="output", help="Output directory")
@click.option("--hive-metastore", default=None, help="Hive metastore JDBC URI")
@click.option("--hdfs-namenode", default=None, help="HDFS namenode URI")
@click.option("--config-dir", default=None, help="Additional config files directory")
def main(repo: str, config: str, out: str, hive_metastore: str, hdfs_namenode: str, config_dir: str) -> None:
    """Spark Lineage Analysis Tool.
    
    Scans repositories for Spark, Hive, Shell, and NiFi code to extract lineage relationships.
    """
    console.print("[bold blue]Spark Lineage Analysis Tool[/bold blue]")
    console.print()
    
    # Load configuration
    cfg = load_config(config) if config else LineageConfig.load_default()
    cfg.repo_path = repo
    cfg.output_dir = out
    
    if hive_metastore:
        cfg.hive_metastore.enabled = True
        cfg.hive_metastore.jdbc_uri = hive_metastore
    
    if config_dir:
        cfg.config_dir = config_dir
    
    repo_path = Path(repo)
    output_dir = Path(out)
    
    # Clean up existing output directory if it exists
    if output_dir.exists():
        import shutil
        console.print(f"[yellow]Cleaning up existing output directory: {output_dir}[/yellow]")
        shutil.rmtree(output_dir)
    
    # Create fresh output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console
        ) as progress:
            
            # Step 1: Crawl repository
            task = progress.add_task("[cyan]Crawling repository...", total=None)
            crawler = FileCrawler(
                repo_path,
                ignore_patterns=cfg.ignore_patterns.paths,
                ignore_extensions=cfg.ignore_patterns.extensions
            )
            files_by_type = crawler.get_files_by_type()
            progress.update(task, completed=True)
            
            crawl_stats = crawler.get_stats()
            console.print(f"✓ Found {crawl_stats['total_files']} files")
            
            # Step 2: Extract facts
            task = progress.add_task("[cyan]Extracting facts...", total=None)
            fact_store = FactStore()
            rule_engine = create_default_engine()
            
            # Extract from each file type
            extractors = {
                FileType.PYSPARK: PySparkExtractor(rule_engine),
                FileType.SCALA: ScalaExtractor(rule_engine),
                FileType.HIVE: HiveExtractor(rule_engine),
                FileType.SHELL: ShellExtractor(rule_engine),
                FileType.NIFI: NiFiExtractor(),
                FileType.PROPERTIES: ConfigExtractor(),
                FileType.YAML: ConfigExtractor(),
                FileType.JSON: ConfigExtractor(),
                FileType.XML: ConfigExtractor(),
            }
            
            for file_type, files in files_by_type.items():
                if file_type in extractors:
                    extractor = extractors[file_type]
                    for crawled_file in files:
                        try:
                            facts = extractor.extract(crawled_file.path)
                            fact_store.add_facts(facts)
                        except Exception as e:
                            console.print(f"[yellow]Warning: Failed to extract from {crawled_file.path}: {e}[/yellow]")
            
            progress.update(task, completed=True)
            
            fact_stats = fact_store.get_stats()
            console.print(f"✓ Extracted {fact_stats['total_facts']} facts from {fact_stats['unique_files']} files")
            
            # Step 3: Build symbol table and resolve variables
            task = progress.add_task("[cyan]Resolving variables...", total=None)
            symbol_table = SymbolTable()
            
            # Add config facts to symbol table
            from lineage.ir import FactType
            for fact in fact_store.get_facts_by_type(FactType.CONFIG):
                if hasattr(fact, 'config_key') and hasattr(fact, 'config_value'):
                    symbol_table.add_property(fact.config_key, fact.config_value)
            
            canonicalizer = PathCanonicalizer(cfg.environments.get(cfg.active_environment, {}).get('base_dirs', {}))
            resolver = VariableResolver(symbol_table, canonicalizer)
            progress.update(task, completed=True)
            
            # Step 4: Build lineage graph
            task = progress.add_task("[cyan]Building lineage graph...", total=None)
            builder = LineageBuilder(fact_store, resolver, ignore_temp_paths=True)
            graph = builder.build()
            progress.update(task, completed=True)
            
            graph_stats = graph.get_stats()
            console.print(f"✓ Built graph with {graph_stats['total_nodes']} nodes and {graph_stats['total_edges']} edges")
            
            # Step 5: Calculate scores and priorities
            task = progress.add_task("[cyan]Calculating priorities...", total=None)
            scorer = ConfidenceScorer()
            priority_calc = PriorityCalculator(graph)
            metrics = priority_calc.calculate_all()
            progress.update(task, completed=True)
            
            # Step 6: Export results
            task = progress.add_task("[cyan]Exporting results...", total=None)
            
            if cfg.export.export_json:
                json_exporter = JSONExporter()
                json_exporter.export(graph, metrics, output_dir / "lineage.json")
            
            if cfg.export.csv:
                csv_exporter = CSVExporter()
                csv_exporter.export(graph, metrics, output_dir / "csv")
            
            if cfg.export.html:
                html_exporter = HTMLExporter()
                html_exporter.export(graph, metrics, output_dir / "lineage_report.html")
            
            # Always export Excel
            try:
                excel_exporter = ExcelExporter(graph, metrics)
                excel_exporter.export(output_dir)
            except ImportError as e:
                console.print(f"[yellow]Warning: Could not export Excel: {e}[/yellow]")
            
            if cfg.export.database and cfg.database.enabled:
                db_exporter = DatabaseExporter(cfg.database)
                db_exporter.export(graph, metrics)
            
            progress.update(task, completed=True)
        
        # Display summary
        console.print()
        console.print("[bold green]✓ Lineage analysis complete![/bold green]")
        console.print()
        
        # Show top priority datasets
        top_priorities = priority_calc.get_top_priority_nodes(10)
        
        table = Table(title="Top 10 Priority Datasets")
        table.add_column("Dataset", style="cyan")
        table.add_column("Priority", justify="right", style="green")
        table.add_column("Wave", justify="center")
        table.add_column("Reach", justify="right")
        table.add_column("Confidence", justify="right")
        
        for m in top_priorities:
            node = graph.get_node_by_id(m.node_id)
            if node:
                table.add_row(
                    node.name[:50],
                    f"{m.priority_score:.1f}",
                    f"{m.migration_wave}",
                    str(m.downstream_reach),
                    f"{m.avg_confidence:.2f}"
                )
        
        console.print(table)
        console.print()
        console.print(f"[dim]Results saved to: {output_dir.absolute()}[/dim]")
    
    except Exception as e:
        console.print(f"[bold red]Error: {e}[/bold red]")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

