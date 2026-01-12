"""
PySpark job with comprehensive UDF usage patterns
Covers: Python UDFs, Pandas UDFs, UDAFs, vectorized operations
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window
import sys

# Import for Pandas UDF
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd


# ========== Define Python UDFs ==========

def parse_json_string(json_str):
    """Simple Python UDF to parse JSON"""
    import json
    try:
        return json.loads(json_str) if json_str else {}
    except:
        return {}

def calculate_discount(price, discount_rate, customer_tier):
    """Business logic UDF with multiple parameters"""
    if customer_tier == "GOLD":
        return price * (1 - discount_rate * 1.5)
    elif customer_tier == "SILVER":
        return price * (1 - discount_rate * 1.2)
    else:
        return price * (1 - discount_rate)

def mask_pii(value, mask_type):
    """UDF for masking PII data"""
    if not value:
        return None
    if mask_type == "email":
        parts = value.split("@")
        return f"{'*' * (len(parts[0])-2)}{parts[0][-2:]}@{parts[1]}" if len(parts) == 2 else value
    elif mask_type == "phone":
        return f"***-***-{value[-4:]}" if len(value) >= 4 else "***"
    else:
        return "*" * len(value)

def validate_credit_card(card_number):
    """Luhn algorithm for credit card validation"""
    if not card_number or not card_number.isdigit():
        return False
    # Simplified Luhn check
    digits = [int(d) for d in card_number]
    checksum = sum(digits[-1::-2] + [sum(divmod(d*2, 10)) for d in digits[-2::-2]])
    return checksum % 10 == 0


# ========== Define Pandas UDFs (Vectorized) ==========

@pandas_udf(DoubleType())
def calculate_tax_pandas(price: pd.Series, state: pd.Series) -> pd.Series:
    """Pandas UDF for tax calculation - vectorized operation"""
    tax_rates = {"CA": 0.0725, "NY": 0.08875, "TX": 0.0625, "FL": 0.06}
    return price * state.map(lambda s: tax_rates.get(s, 0.05))

@pandas_udf(StringType())
def normalize_address_pandas(address: pd.Series) -> pd.Series:
    """Pandas UDF for address normalization"""
    return address.str.upper().str.replace(r'\s+', ' ', regex=True).str.strip()

@pandas_udf(StructType([
    StructField("is_valid", BooleanType()),
    StructField("score", DoubleType()),
    StructField("category", StringType())
]))
def fraud_detection_pandas(transaction_amount: pd.Series, 
                          avg_amount: pd.Series,
                          transaction_count: pd.Series) -> pd.DataFrame:
    """Complex Pandas UDF returning struct"""
    df = pd.DataFrame()
    df['is_valid'] = (transaction_amount < avg_amount * 10) & (transaction_count < 100)
    df['score'] = (transaction_amount / (avg_amount + 1)) * (transaction_count / 10)
    df['category'] = pd.cut(df['score'], bins=[0, 1, 5, 10, float('inf')], 
                            labels=['LOW', 'MEDIUM', 'HIGH', 'CRITICAL'])
    return df

@pandas_udf(ArrayType(StringType()))
def extract_keywords_pandas(text: pd.Series) -> pd.Series:
    """Pandas UDF returning array"""
    import re
    return text.apply(lambda x: re.findall(r'\b[A-Z][a-z]+\b', x) if x else [])


def main(run_date, env):
    spark = SparkSession.builder \
        .appName("UDF_Comprehensive_Job") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    
    base_path = f"/data/{env}"
    
    # ========== Register UDFs ==========
    
    # Register Python UDFs
    parse_json_udf = udf(parse_json_string, MapType(StringType(), StringType()))
    calculate_discount_udf = udf(calculate_discount, DoubleType())
    mask_pii_udf = udf(mask_pii, StringType())
    validate_cc_udf = udf(validate_credit_card, BooleanType())
    
    # Register UDFs for SQL usage
    spark.udf.register("parse_json_sql", parse_json_string, MapType(StringType(), StringType()))
    spark.udf.register("calculate_discount_sql", calculate_discount, DoubleType())
    spark.udf.register("mask_pii_sql", mask_pii, StringType())
    
    # ========== Read Data from Multiple Sources ==========
    
    # Read from Parquet
    df_transactions = spark.read.parquet(f"{base_path}/raw/transactions/{run_date}")
    
    # Read from Hive tables
    df_customers = spark.table(f"{env}_warehouse.customers")
    df_products = spark.table(f"{env}_warehouse.products")
    
    # Read JSON with nested structures
    df_events = spark.read.json(f"{base_path}/raw/events/{run_date}/*.json")
    
    # ========== Apply Python UDFs ==========
    
    # Parse JSON column using UDF
    df_events_parsed = df_events.withColumn(
        "payload_parsed",
        parse_json_udf(col("payload"))
    )
    
    # Apply discount calculation UDF
    df_with_discount = df_transactions.join(df_customers, "customer_id").withColumn(
        "discounted_price",
        calculate_discount_udf(col("price"), col("discount_rate"), col("customer_tier"))
    )
    
    # Mask PII data
    df_customers_masked = df_customers \
        .withColumn("email_masked", mask_pii_udf(col("email"), lit("email"))) \
        .withColumn("phone_masked", mask_pii_udf(col("phone"), lit("phone")))
    
    # Validate credit cards
    df_payments = spark.read.parquet(f"{base_path}/raw/payments/{run_date}")
    df_payments_validated = df_payments.withColumn(
        "is_valid_card",
        validate_cc_udf(col("card_number"))
    )
    
    # ========== Apply Pandas UDFs (Vectorized) ==========
    
    # Calculate tax using Pandas UDF
    df_with_tax = df_transactions.withColumn(
        "tax_amount",
        calculate_tax_pandas(col("price"), col("state"))
    )
    
    # Normalize addresses
    df_customers_normalized = df_customers.withColumn(
        "address_normalized",
        normalize_address_pandas(col("address"))
    )
    
    # Apply fraud detection (returns struct)
    window_spec = Window.partitionBy("customer_id").orderBy("transaction_date")
    df_fraud_check = df_transactions \
        .withColumn("avg_amount", avg("amount").over(window_spec)) \
        .withColumn("transaction_count", count("*").over(window_spec)) \
        .withColumn(
            "fraud_check",
            fraud_detection_pandas(col("amount"), col("avg_amount"), col("transaction_count"))
        )
    
    # Extract keywords from text
    df_reviews = spark.read.text(f"{base_path}/raw/reviews/{run_date}/*.txt")
    df_reviews_with_keywords = df_reviews.withColumn(
        "keywords",
        extract_keywords_pandas(col("value"))
    )
    
    # ========== Use UDFs in SQL Context ==========
    
    df_transactions.createOrReplaceTempView("transactions")
    df_customers.createOrReplaceTempView("customers")
    
    df_sql_result = spark.sql("""
        SELECT 
            t.transaction_id,
            t.customer_id,
            c.customer_name,
            t.price,
            calculate_discount_sql(t.price, t.discount_rate, c.customer_tier) as final_price,
            mask_pii_sql(c.email, 'email') as masked_email,
            t.transaction_date
        FROM transactions t
        JOIN customers c ON t.customer_id = c.customer_id
        WHERE t.transaction_date = '{run_date}'
    """)
    
    # ========== Complex UDF Pipeline ==========
    
    # Chain multiple UDFs
    df_complex = df_transactions \
        .join(df_customers, "customer_id") \
        .join(df_products, "product_id") \
        .withColumn("discounted_price", calculate_discount_udf(col("price"), lit(0.1), col("customer_tier"))) \
        .withColumn("tax_amount", calculate_tax_pandas(col("discounted_price"), col("state"))) \
        .withColumn("final_price", col("discounted_price") + col("tax_amount")) \
        .withColumn("email_masked", mask_pii_udf(col("email"), lit("email"))) \
        .withColumn("phone_masked", mask_pii_udf(col("phone"), lit("phone")))
    
    # ========== Write Results to Multiple Destinations ==========
    
    # Write to Parquet with UDF-transformed data
    df_complex.write \
        .mode("overwrite") \
        .partitionBy("transaction_date", "state") \
        .parquet(f"{base_path}/processed/transactions_with_udfs/{run_date}")
    
    # Write masked data to JSON
    df_customers_masked.write \
        .mode("overwrite") \
        .json(f"{base_path}/processed/customers_masked/{run_date}")
    
    # Write fraud detection results to Avro
    df_fraud_check.write \
        .format("avro") \
        .mode("overwrite") \
        .save(f"{base_path}/processed/fraud_detection/{run_date}")
    
    # Write to Hive tables
    df_sql_result.write \
        .mode("overwrite") \
        .saveAsTable(f"{env}_analytics.transactions_processed")
    
    df_complex.write \
        .mode("append") \
        .insertInto(f"{env}_analytics.customer_transactions_daily")
    
    # Write aggregated results with UDFs to ORC
    df_summary = df_complex.groupBy("customer_id", "state").agg(
        count("*").alias("transaction_count"),
        sum("final_price").alias("total_spent"),
        avg("final_price").alias("avg_transaction")
    )
    
    df_summary.write \
        .mode("overwrite") \
        .orc(f"{base_path}/processed/customer_summary/{run_date}")
    
    # Write to Hive with ORC format
    df_summary.write \
        .mode("overwrite") \
        .format("orc") \
        .saveAsTable(f"{env}_reports.customer_summary_with_udfs")
    
    spark.stop()


if __name__ == "__main__":
    run_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
    env = sys.argv[2] if len(sys.argv) > 2 else "prod"
    main(run_date, env)

