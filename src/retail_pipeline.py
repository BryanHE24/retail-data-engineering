from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, when

from src.transformations import (
    prepare_transaction_types,
    add_rejection_reason,
    add_line_amount,
)

spark = SparkSession.builder.getOrCreate()

# Read the source path supplied by the bundle's pipeline configuration.
landing_path = spark.conf.get("retail.landing_path")


# Golden aggegations
@dp.materialized_view(name="gold_daily_sales")
def gold_daily_sales():
    """Summarize accepted transactions by invoice date."""

    # Read Silver and calculate the same daily totals as Demo 2.
    # Cancellation amounts stay negative and reduce the net amount.
    return spark.sql("""
        SELECT
            CAST(invoice_timestamp AS DATE) AS sales_date,

            SUM(CASE WHEN transaction_type = 'sale'
                THEN line_amount ELSE 0 END) AS sales_amount,

            SUM(CASE WHEN transaction_type = 'cancellation'
                THEN line_amount ELSE 0 END) AS cancellation_amount,

            SUM(line_amount) AS net_amount

        FROM silver_online_retail
        GROUP BY CAST(invoice_timestamp AS DATE)
    """)


# DP table decorator registers this function as a streaming ingestion bronze table in the pipeline.
@dp.table(name="bronze_online_retail")
def bronze_online_retail():
    """Ingest the retail CSV and preserve source traceability."""

    # STEP 1: Discover and read source files with Auto Loader.
    # Keep CSV fields as strings; our transformations handle conversion.
    incoming = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .load(landing_path)
    )

    # STEP 2: Add metadata identifying the source and processing time.
    return incoming.select(
        "*",
        col("_metadata.file_path").alias("_source_file"),
        current_timestamp().alias("_ingestion_timestamp")
    )

# Temoporaly view retaoil_checked is used to prepare and check records before separating the outputs.
@dp.temporary_view(name="retail_checked")
@dp.expect("valid_transaction", "rejection_reason IS NULL")
def retail_checked():
    """Prepare and check records before separating the outputs."""

    # Read Bronze from this pipeline's target schema.
    bronze = spark.readStream.table("bronze_online_retail")

    # Reuse the same functions exercised by our unit tests.
    typed = prepare_transaction_types(bronze)
    return add_rejection_reason(typed)

# Table used for accepted transactions and their amounts.
@dp.table(name="silver_online_retail")
def silver_online_retail():
    """Keep accepted transactions and calculate their amounts."""

    # Only rows without a rejection reason belong in Silver.
    accepted = (
        spark.readStream.table("retail_checked")
        .filter(col("rejection_reason").isNull())
    )

    # Classify accepted records using the normalized invoice number.
    classified = accepted.withColumn(
        "transaction_type",
        when(col("invoice_no").startswith("C"), "cancellation")
        .otherwise("sale")
    )

    # Reuse our tested calculation, preserving negative amounts.
    return add_line_amount(classified)


# Table used for rejected transactions and their reasons.
@dp.table(name="quarantine_online_retail")
def quarantine_online_retail():
    """Keep rejected records and their reasons for investigation."""

    return (
        spark.readStream.table("retail_checked")
        .filter(col("rejection_reason").isNotNull())
    )


