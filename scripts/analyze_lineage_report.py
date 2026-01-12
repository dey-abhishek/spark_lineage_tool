"""
Sample analytics script showing how to programmatically analyze the lineage Excel report.

This demonstrates various analytics you can perform using Python + pandas + openpyxl.
"""

import pandas as pd
from pathlib import Path
from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns

# Set up paths
REPORT_PATH = Path("output/comprehensive_report/lineage_report.xlsx")


def load_analytics_data():
    """Load all sheets from the Excel report."""
    print("📊 Loading lineage report...")
    
    sheets = {
        'summary': pd.read_excel(REPORT_PATH, sheet_name='Summary', header=None),
        'source_analysis': pd.read_excel(REPORT_PATH, sheet_name='Source Analysis', skiprows=2),
        'analytics': pd.read_excel(REPORT_PATH, sheet_name='Analytics (Pivot-Ready)', skiprows=2),
        'datasets': pd.read_excel(REPORT_PATH, sheet_name='All Datasets'),
        'jobs': pd.read_excel(REPORT_PATH, sheet_name='All Jobs'),
        'metrics': pd.read_excel(REPORT_PATH, sheet_name='Detailed Metrics'),
    }
    
    print(f"✓ Loaded {len(sheets)} sheets\n")
    return sheets


def analyze_relationships_per_source(df_analytics):
    """Count total relationships per source file."""
    print("=" * 80)
    print("📁 RELATIONSHIPS PER SOURCE")
    print("=" * 80)
    
    source_counts = df_analytics['Source File'].value_counts().head(10)
    
    print("\nTop 10 Sources by Relationship Count:")
    print("-" * 60)
    for source, count in source_counts.items():
        print(f"  {source:40s} : {count:4d} relationships")
    
    return source_counts


def analyze_by_technology(df_analytics):
    """Analyze relationships by technology type."""
    print("\n" + "=" * 80)
    print("💻 TECHNOLOGY BREAKDOWN")
    print("=" * 80)
    
    tech_stats = df_analytics.groupby('Source Type').agg({
        'Relationship': 'count',
        'Confidence': 'mean',
        'Source File': 'nunique'
    }).round(2)
    
    tech_stats.columns = ['Total Relationships', 'Avg Confidence', 'Unique Files']
    tech_stats = tech_stats.sort_values('Total Relationships', ascending=False)
    
    print("\nTechnology Usage Summary:")
    print("-" * 60)
    print(tech_stats.to_string())
    
    return tech_stats


def analyze_read_write_patterns(df_analytics):
    """Analyze READ vs WRITE patterns."""
    print("\n" + "=" * 80)
    print("📖 READ vs WRITE PATTERNS")
    print("=" * 80)
    
    pivot = df_analytics.groupby(['Source Type', 'Relationship']).size().unstack(fill_value=0)
    
    print("\nRead/Write Breakdown by Technology:")
    print("-" * 60)
    print(pivot.to_string())
    
    # Calculate ratios
    if 'READ' in pivot.columns and 'WRITE' in pivot.columns:
        pivot['Ratio (R:W)'] = (pivot['READ'] / pivot['WRITE']).round(2)
        print("\n\nRead-to-Write Ratios:")
        print("-" * 60)
        print(pivot[['READ', 'WRITE', 'Ratio (R:W)']].to_string())
    
    return pivot


def analyze_confidence_distribution(df_analytics):
    """Analyze confidence score distribution."""
    print("\n" + "=" * 80)
    print("🎯 CONFIDENCE DISTRIBUTION")
    print("=" * 80)
    
    # Define confidence bins
    bins = [0, 0.5, 0.7, 0.85, 1.0]
    labels = ['Low (<0.5)', 'Medium (0.5-0.7)', 'High (0.7-0.85)', 'Very High (>0.85)']
    
    df_analytics['Confidence_Band'] = pd.cut(df_analytics['Confidence'], bins=bins, labels=labels)
    
    dist = df_analytics['Confidence_Band'].value_counts().sort_index()
    
    print("\nConfidence Score Distribution:")
    print("-" * 60)
    for band, count in dist.items():
        pct = (count / len(df_analytics)) * 100
        bar = '█' * int(pct / 2)
        print(f"  {band:20s} : {count:4d} ({pct:5.1f}%) {bar}")
    
    print(f"\nOverall Average Confidence: {df_analytics['Confidence'].mean():.2f}")
    
    return dist


def analyze_top_datasets(df_analytics):
    """Find datasets touched by the most sources."""
    print("\n" + "=" * 80)
    print("🌟 MOST REFERENCED DATASETS")
    print("=" * 80)
    
    dataset_sources = df_analytics.groupby('Target Dataset').agg({
        'Source File': 'nunique',
        'Relationship': lambda x: f"{sum(x=='READ')}R/{sum(x=='WRITE')}W"
    })
    
    dataset_sources.columns = ['# of Sources', 'Read/Write']
    dataset_sources = dataset_sources.sort_values('# of Sources', ascending=False).head(10)
    
    print("\nTop 10 Datasets by Source Count:")
    print("-" * 60)
    print(dataset_sources.to_string())
    
    return dataset_sources


def analyze_migration_waves(df_analytics):
    """Analyze relationships by migration wave."""
    print("\n" + "=" * 80)
    print("🌊 MIGRATION WAVE ANALYSIS")
    print("=" * 80)
    
    # Filter out N/A waves
    wave_data = df_analytics[df_analytics['Wave'] != 'N/A'].copy()
    
    if len(wave_data) > 0:
        wave_tech = wave_data.groupby(['Wave', 'Source Type']).size().unstack(fill_value=0)
        
        print("\nRelationships per Wave by Technology:")
        print("-" * 60)
        print(wave_tech.to_string())
        
        wave_totals = wave_data['Wave'].value_counts().sort_index()
        print("\n\nTotal Relationships per Wave:")
        print("-" * 60)
        for wave, count in wave_totals.items():
            print(f"  {wave:10s} : {count:4d} relationships")
    else:
        print("\nNo wave data available")
    
    return wave_data


def analyze_low_confidence_sources(df_source_analysis):
    """Identify sources with low confidence that need attention."""
    print("\n" + "=" * 80)
    print("⚠️  LOW CONFIDENCE SOURCES (Need Review)")
    print("=" * 80)
    
    low_conf = df_source_analysis[df_source_analysis['Avg Confidence'] < 0.7].copy()
    low_conf = low_conf.sort_values('Total Relationships', ascending=False)
    
    if len(low_conf) > 0:
        print(f"\nFound {len(low_conf)} sources with confidence < 0.7:")
        print("-" * 60)
        
        for _, row in low_conf.head(10).iterrows():
            print(f"  {row['Source File/System']:40s} | "
                  f"Conf: {row['Avg Confidence']:.2f} | "
                  f"Rels: {int(row['Total Relationships']):3d} | "
                  f"Type: {row['Type']}")
    else:
        print("\n✓ No low confidence sources found!")
    
    return low_conf


def generate_summary_report(sheets):
    """Generate a comprehensive summary report."""
    print("\n" + "=" * 80)
    print("📋 EXECUTIVE SUMMARY")
    print("=" * 80)
    
    df_analytics = sheets['analytics']
    df_source_analysis = sheets['source_analysis']
    
    total_sources = len(df_source_analysis)
    total_relationships = len(df_analytics)
    avg_conf = df_analytics['Confidence'].mean()
    
    # Count by technology
    tech_counts = df_source_analysis['Type'].value_counts()
    
    # Confidence breakdown
    high_conf = len(df_analytics[df_analytics['Confidence'] >= 0.8])
    med_conf = len(df_analytics[(df_analytics['Confidence'] >= 0.6) & (df_analytics['Confidence'] < 0.8)])
    low_conf = len(df_analytics[df_analytics['Confidence'] < 0.6])
    
    print(f"""
OVERALL STATISTICS
{'─' * 60}
Total Source Files       : {total_sources:,}
Total Relationships      : {total_relationships:,}
Average Confidence       : {avg_conf:.2f}

TECHNOLOGY BREAKDOWN
{'─' * 60}""")
    for tech, count in tech_counts.items():
        pct = (count / total_sources) * 100
        print(f"  {str(tech):20s} : {int(count):3d} sources ({pct:5.1f}%)")
    
    print(f"""
CONFIDENCE BREAKDOWN
{'─' * 60}
High (≥0.8)              : {high_conf:,} ({high_conf/total_relationships*100:.1f}%)
Medium (0.6-0.8)         : {med_conf:,} ({med_conf/total_relationships*100:.1f}%)
Low (<0.6)               : {low_conf:,} ({low_conf/total_relationships*100:.1f}%)

ACTION ITEMS
{'─' * 60}
✓ {high_conf:,} relationships are production-ready
⚠ {med_conf:,} relationships need validation
✗ {low_conf:,} relationships require investigation
    """)


def create_visualizations(df_analytics, df_source_analysis):
    """Create summary visualizations (optional - requires matplotlib)."""
    print("\n" + "=" * 80)
    print("📊 GENERATING VISUALIZATIONS")
    print("=" * 80)
    
    try:
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        fig.suptitle('Lineage Analytics Dashboard', fontsize=16, fontweight='bold')
        
        # 1. Technology breakdown
        tech_counts = df_source_analysis['Type'].value_counts()
        axes[0, 0].pie(tech_counts.values, labels=tech_counts.index, autopct='%1.1f%%')
        axes[0, 0].set_title('Source Files by Technology')
        
        # 2. Confidence distribution
        axes[0, 1].hist(df_analytics['Confidence'], bins=20, edgecolor='black')
        axes[0, 1].set_xlabel('Confidence Score')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].set_title('Confidence Score Distribution')
        axes[0, 1].axvline(0.8, color='red', linestyle='--', label='High Threshold')
        axes[0, 1].legend()
        
        # 3. Read vs Write by Technology
        pivot = df_analytics.groupby(['Source Type', 'Relationship']).size().unstack(fill_value=0)
        pivot.plot(kind='bar', ax=axes[1, 0], stacked=False)
        axes[1, 0].set_title('Read vs Write by Technology')
        axes[1, 0].set_xlabel('Technology')
        axes[1, 0].set_ylabel('Count')
        axes[1, 0].tick_params(axis='x', rotation=45)
        
        # 4. Top sources by relationship count
        top_sources = df_analytics['Source File'].value_counts().head(10)
        top_sources.plot(kind='barh', ax=axes[1, 1])
        axes[1, 1].set_title('Top 10 Sources by Relationship Count')
        axes[1, 1].set_xlabel('Relationship Count')
        
        plt.tight_layout()
        
        output_file = "output/comprehensive_report/analytics_dashboard.png"
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"\n✓ Saved dashboard to: {output_file}")
        
    except Exception as e:
        print(f"\n⚠ Could not generate visualizations: {e}")
        print("  (Install matplotlib/seaborn if you want charts)")


def main():
    """Run all analytics."""
    print("\n" + "╔" + "═" * 78 + "╗")
    print("║" + " " * 20 + "LINEAGE ANALYTICS REPORT" + " " * 34 + "║")
    print("╚" + "═" * 78 + "╝\n")
    
    # Check if report exists
    if not REPORT_PATH.exists():
        print(f"❌ Error: Report not found at {REPORT_PATH}")
        print("\nPlease run: python -m lineage.cli --repo <path> --out output/comprehensive_report")
        return
    
    # Load data
    sheets = load_analytics_data()
    
    # Run all analyses
    analyze_relationships_per_source(sheets['analytics'])
    analyze_by_technology(sheets['analytics'])
    analyze_read_write_patterns(sheets['analytics'])
    analyze_confidence_distribution(sheets['analytics'])
    analyze_top_datasets(sheets['analytics'])
    analyze_migration_waves(sheets['analytics'])
    analyze_low_confidence_sources(sheets['source_analysis'])
    generate_summary_report(sheets)
    
    # Create visualizations
    create_visualizations(sheets['analytics'], sheets['source_analysis'])
    
    print("\n" + "=" * 80)
    print("✓ Analytics complete! Review the output above.")
    print("=" * 80)
    print(f"\n📄 Full report available at: {REPORT_PATH}")
    print("💡 Tip: Open in Excel and create pivot tables for deeper analysis\n")


if __name__ == "__main__":
    main()

