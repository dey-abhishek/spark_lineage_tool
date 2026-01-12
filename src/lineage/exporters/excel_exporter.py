"""Excel exporter for lineage data with multiple formatted sheets."""

from pathlib import Path
from typing import Dict, List
import json

try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

from lineage.lineage import LineageGraph, NodeType


class ExcelExporter:
    """Export lineage data to Excel with multiple formatted sheets."""
    
    def __init__(self, graph: LineageGraph, metrics: Dict = None):
        self.graph = graph
        self.metrics = metrics or {}
        if not EXCEL_AVAILABLE:
            raise ImportError("openpyxl is required for Excel export. Install with: pip install openpyxl")
    
    def export(self, output_path: Path) -> None:
        """Export to Excel workbook with multiple sheets."""
        wb = Workbook()
        wb.remove(wb.active)  # Remove default sheet
        
        # Create sheets
        self._create_summary_sheet(wb)
        self._create_priority_sheet(wb)
        self._create_datasets_sheet(wb)
        self._create_jobs_sheet(wb)
        self._create_lineage_sheet(wb)
        self._create_metrics_sheet(wb)
        
        # Save workbook
        output_file = output_path / "lineage_report.xlsx"
        wb.save(output_file)
        print(f"Exported Excel report to {output_file}")
    
    def _create_summary_sheet(self, wb: Workbook) -> None:
        """Create summary overview sheet."""
        ws = wb.create_sheet("Summary", 0)
        
        # Title
        ws['A1'] = "Lineage Analysis Summary Report"
        ws['A1'].font = Font(size=16, bold=True, color="FFFFFF")
        ws['A1'].fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
        ws.merge_cells('A1:D1')
        
        # Statistics
        row = 3
        graph_stats = self.graph.get_stats()
        stats = [
            ("Total Nodes", graph_stats.get('total_nodes', 0)),
            ("Total Edges", graph_stats.get('total_edges', 0)),
            ("Dataset Nodes", len(self.graph.get_nodes_by_type(NodeType.DATASET))),
            ("Job Nodes", len(self.graph.get_nodes_by_type(NodeType.JOB))),
            ("Module Nodes", len(self.graph.get_nodes_by_type(NodeType.MODULE))),
        ]
        
        ws[f'A{row}'] = "Metric"
        ws[f'B{row}'] = "Count"
        self._style_header_row(ws, row, 2)
        
        for stat_name, stat_value in stats:
            row += 1
            ws[f'A{row}'] = stat_name
            ws[f'B{row}'] = stat_value
        
        # Metrics summary if available
        if self.metrics:
            row += 2
            ws[f'A{row}'] = "Priority Analysis"
            ws[f'A{row}'].font = Font(bold=True, size=12)
            row += 1
            
            wave_counts = {}
            for metric in self.metrics.values():
                wave = metric.migration_wave
                wave_counts[wave] = wave_counts.get(wave, 0) + 1
            
            ws[f'A{row}'] = "Migration Wave"
            ws[f'B{row}'] = "Datasets"
            self._style_header_row(ws, row, 2)
            
            for wave in sorted(wave_counts.keys()):
                row += 1
                ws[f'A{row}'] = f"Wave {wave}"
                ws[f'B{row}'] = wave_counts[wave]
        
        # Auto-size columns
        ws.column_dimensions['A'].width = 25
        ws.column_dimensions['B'].width = 15
    
    def _create_priority_sheet(self, wb: Workbook) -> None:
        """Create top priority datasets sheet."""
        ws = wb.create_sheet("Top Priorities")
        
        # Header
        ws['A1'] = "Top Priority Datasets for Migration"
        ws['A1'].font = Font(size=14, bold=True)
        ws.merge_cells('A1:G1')
        
        # Column headers
        headers = ["Rank", "Dataset Name", "Priority Score", "Wave", "Downstream Reach", "Fan Out", "Confidence"]
        for col, header in enumerate(headers, 1):
            cell = ws.cell(3, col, header)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
            cell.alignment = Alignment(horizontal="center", vertical="center")
        
        # Get top datasets
        if self.metrics:
            sorted_metrics = sorted(
                self.metrics.items(),
                key=lambda x: x[1].priority_score,
                reverse=True
            )[:50]  # Top 50
            
            row = 4
            for rank, (node_id, metric) in enumerate(sorted_metrics, 1):
                # Find node by ID
                node = self.graph.get_node_by_id(node_id)
                
                if not node:
                    continue
                
                # Determine wave color
                if metric.migration_wave == 1:
                    fill_color = "FFE6E6"  # Light red
                elif metric.migration_wave == 2:
                    fill_color = "FFF4E6"  # Light yellow
                else:
                    fill_color = "E6F4EA"  # Light green
                
                ws.cell(row, 1, rank)
                ws.cell(row, 2, node.name)
                ws.cell(row, 3, round(metric.priority_score, 2))
                ws.cell(row, 4, f"Wave {metric.migration_wave}")
                ws.cell(row, 5, metric.downstream_reach)
                ws.cell(row, 6, metric.fan_out)
                ws.cell(row, 7, round(metric.avg_confidence, 2))
                
                # Color-code by wave
                for col in range(1, 8):
                    ws.cell(row, col).fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
                
                row += 1
        
        # Auto-size columns
        for col in range(1, 8):
            ws.column_dimensions[get_column_letter(col)].width = 18
    
    def _create_datasets_sheet(self, wb: Workbook) -> None:
        """Create all datasets sheet."""
        ws = wb.create_sheet("All Datasets")
        
        # Headers
        headers = ["Dataset Name", "URN", "Type", "Fully Resolved", "Fact Count", "Upstream Jobs", "Downstream Jobs"]
        for col, header in enumerate(headers, 1):
            cell = ws.cell(1, col, header)
            self._style_header_row(ws, 1, len(headers))
        
        # Data
        row = 2
        for node in self.graph.get_nodes_by_type(NodeType.DATASET):
            ws.cell(row, 1, node.name)
            ws.cell(row, 2, node.urn)
            ws.cell(row, 3, node.metadata.get('dataset_type', 'unknown'))
            ws.cell(row, 4, "Yes" if node.metadata.get('fully_resolved', False) else "No")
            ws.cell(row, 5, node.metadata.get('fact_count', 0))
            ws.cell(row, 6, self.graph.get_fan_in(node.node_id))
            ws.cell(row, 7, self.graph.get_fan_out(node.node_id))
            row += 1
        
        # Auto-size columns
        for col in range(1, len(headers) + 1):
            ws.column_dimensions[get_column_letter(col)].width = 25
    
    def _create_jobs_sheet(self, wb: Workbook) -> None:
        """Create all jobs sheet."""
        ws = wb.create_sheet("All Jobs")
        
        # Headers
        headers = ["Job Name", "Source File", "Reads From", "Writes To"]
        for col, header in enumerate(headers, 1):
            self._style_header_row(ws, 1, len(headers))
            ws.cell(1, col, header)
        
        # Data
        row = 2
        for node in self.graph.get_nodes_by_type(NodeType.JOB):
            source_file = node.metadata.get('source_file', '')
            
            # Count inputs/outputs
            upstream_count = len([e for e in self.graph.edges if e.target_node_id == node.node_id])
            downstream_count = len([e for e in self.graph.edges if e.source_node_id == node.node_id])
            
            ws.cell(row, 1, node.name)
            ws.cell(row, 2, source_file)
            ws.cell(row, 3, f"{upstream_count} datasets")
            ws.cell(row, 4, f"{downstream_count} datasets")
            row += 1
        
        # Auto-size columns
        for col in range(1, len(headers) + 1):
            ws.column_dimensions[get_column_letter(col)].width = 30
    
    def _create_lineage_sheet(self, wb: Workbook) -> None:
        """Create lineage relationships sheet."""
        ws = wb.create_sheet("Lineage Edges")
        
        # Headers
        headers = ["Source", "Relationship", "Target", "Confidence", "Evidence"]
        for col, header in enumerate(headers, 1):
            self._style_header_row(ws, 1, len(headers))
            ws.cell(1, col, header)
        
        # Data (limit to top edges by confidence)
        sorted_edges = sorted(self.graph.edges, key=lambda e: e.confidence, reverse=True)[:1000]
        
        row = 2
        for edge in sorted_edges:
            source_node = self.graph.get_node_by_id(edge.source_node_id)
            target_node = self.graph.get_node_by_id(edge.target_node_id)
            
            if not source_node or not target_node:
                continue
            
            ws.cell(row, 1, source_node.name)
            ws.cell(row, 2, edge.edge_type.value)
            ws.cell(row, 3, target_node.name)
            ws.cell(row, 4, round(edge.confidence, 2))
            ws.cell(row, 5, edge.evidence[:100] if edge.evidence else "")
            
            # Color-code by confidence
            if edge.confidence >= 0.8:
                fill_color = "C6EFCE"  # Green
            elif edge.confidence >= 0.6:
                fill_color = "FFEB9C"  # Yellow
            else:
                fill_color = "FFC7CE"  # Red
            
            ws.cell(row, 4).fill = PatternFill(start_color=fill_color, end_color=fill_color, fill_type="solid")
            
            row += 1
        
        # Auto-size columns
        ws.column_dimensions['A'].width = 30
        ws.column_dimensions['B'].width = 15
        ws.column_dimensions['C'].width = 30
        ws.column_dimensions['D'].width = 12
        ws.column_dimensions['E'].width = 40
    
    def _create_metrics_sheet(self, wb: Workbook) -> None:
        """Create detailed metrics sheet."""
        if not self.metrics:
            return
        
        ws = wb.create_sheet("Detailed Metrics")
        
        # Headers
        headers = ["Dataset", "Fan In", "Fan Out", "Downstream Reach", "Write Jobs", 
                   "Avg Confidence", "Priority Score", "Migration Wave"]
        for col, header in enumerate(headers, 1):
            self._style_header_row(ws, 1, len(headers))
            ws.cell(1, col, header)
        
        # Data
        sorted_metrics = sorted(
            self.metrics.items(),
            key=lambda x: x[1].priority_score,
            reverse=True
        )
        
        row = 2
        for node_id, metric in sorted_metrics:
            node = self.graph.get_node_by_id(node_id)
            if not node:
                continue
            
            ws.cell(row, 1, node.name)
            ws.cell(row, 2, metric.fan_in)
            ws.cell(row, 3, metric.fan_out)
            ws.cell(row, 4, metric.downstream_reach)
            ws.cell(row, 5, metric.write_job_count)
            ws.cell(row, 6, round(metric.avg_confidence, 2))
            ws.cell(row, 7, round(metric.priority_score, 2))
            ws.cell(row, 8, f"Wave {metric.migration_wave}")
            row += 1
        
        # Auto-size columns
        for col in range(1, len(headers) + 1):
            ws.column_dimensions[get_column_letter(col)].width = 18
    
    def _style_header_row(self, ws, row: int, num_cols: int) -> None:
        """Apply consistent styling to header rows."""
        for col in range(1, num_cols + 1):
            cell = ws.cell(row, col)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = Border(
                left=Side(style='thin'),
                right=Side(style='thin'),
                top=Side(style='thin'),
                bottom=Side(style='thin')
            )

