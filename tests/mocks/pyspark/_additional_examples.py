#!/usr/bin/env python3
"""Generate additional PySpark mock files programmatically."""

# This file documents patterns for additional 5+ mock files

examples = [
    {
        "name": "11_streaming.py",
        "pattern": "Structured Streaming with readStream and writeStream",
        "reads": ["/data/streaming/input"],
        "writes": ["/data/streaming/output", "checkpoint://streaming_checkpoint"]
    },
    {
        "name": "12_delta_lake.py",
        "pattern": "Delta Lake operations with merge/upsert",
        "reads": ["/data/delta/source"],
        "writes": ["/data/delta/target"]
    },
    {
        "name": "13_complex_sql.py",
        "pattern": "Complex SQL with CTEs, subqueries, window functions",
        "reads": ["prod.orders", "prod.customers", "prod.products"],
        "writes": ["analytics.order_analysis"]
    },
    {
        "name": "14_incremental_load.py",
        "pattern": "Incremental processing with watermark handling",
        "reads": ["/data/raw/incremental/${date}", "checkpoint://incremental_state"],
        "writes": ["/data/processed/incremental/${date}"]
    },
    {
        "name": "15_error_handling.py",
        "pattern": "Error handling with bad records path",
        "reads": ["/data/raw/with_errors"],
        "writes": ["/data/processed/clean", "/data/errors/bad_records"]
    }
]

print("Additional mock examples documented:")
for ex in examples:
    print(f"  - {ex['name']}: {ex['pattern']}")

