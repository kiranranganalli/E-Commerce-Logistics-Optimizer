"""
ETL Pipeline Script for E-Commerce Logistics Optimizer
------------------------------------------------------
This script simulates an end-to-end ETL workflow to ingest, transform, and load logistics data into Amazon Redshift. 
It processes daily package-level records from S3, applies business logic transformations, joins auxiliary reference data,
and pushes optimized delivery metrics for analytics and reporting.

Technologies Used:
- AWS Glue (via boto3 and pyspark)
- Amazon S3 (input data source)
- Amazon Redshift (data warehouse)
- Python (orchestration and transformation)
- PySpark (distributed transformations)

Author: Kiran Ranganalli
Last Updated: July 2025
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, datediff, expr, when, lit
import boto3
import logging

# Initialize Spark
spark = SparkSession.builder \
    .appName("E-Commerce Logistics ETL") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("logistics_etl")

# Constants
S3_INPUT_PATH = "s3://ecom-logistics-data/raw/2025/"
S3_LOOKUP_PATH = "s3://ecom-logistics-data/reference/"
REDSHIFT_TABLE = "logistics.delivery_metrics"
REDSHIFT_TEMP_DIR = "s3://ecom-logistics-data/temp/"

# Load Raw Package Data
def load_raw_data():
    logger.info("Loading raw package records from S3")
    return spark.read.json(S3_INPUT_PATH)

# Load Location Lookup Table
def load_location_reference():
    logger.info("Loading location reference data")
    return spark.read.csv(S3_LOOKUP_PATH + "location_mapping.csv", header=True)

# Data Transformation Logic
def transform_data(df_packages, df_locations):
    logger.info("Transforming data: applying schema, date conversions, joins")
    df_transformed = df_packages \
        .withColumn("pickup_time", to_timestamp(col("pickup_time"))) \
        .withColumn("delivery_time", to_timestamp(col("delivery_time"))) \
        .withColumn("delivery_duration", datediff("delivery_time", "pickup_time")) \
        .join(df_locations, on="zip_code", how="left") \
        .withColumn("delivery_efficiency", 
                    when(col("delivery_duration") <= 2, lit("High"))
                    .when(col("delivery_duration") <= 5, lit("Medium"))
                    .otherwise(lit("Low"))) \
        .withColumn("cost_to_serve", 
                    expr("base_cost + (delivery_duration * variable_cost_per_day)"))

    df_transformed = df_transformed.withColumn("on_time", 
        when(col("delivery_duration") <= col("sla_days"), lit(True)).otherwise(lit(False)))

    return df_transformed

# Additional Audit Fields
def enrich_with_audit_fields(df):
    logger.info("Adding audit metadata to DataFrame")
    from pyspark.sql.functions import current_timestamp
    return df.withColumn("ingestion_time", current_timestamp())

# Write to Redshift
def load_to_redshift(df_final):
    logger.info("Writing optimized delivery metrics to Redshift")
    df_final.write \
        .format("com.databricks.spark.redshift") \
        .option("url", "jdbc:redshift://<redshift-endpoint>:5439/dev") \
        .option("dbtable", REDSHIFT_TABLE) \
        .option("tempdir", REDSHIFT_TEMP_DIR) \
        .option("aws_iam_role", "arn:aws:iam::<account-id>:role/RedshiftCopyRole") \
        .mode("overwrite") \
        .save()

# Quality Checks
def run_quality_checks(df):
    logger.info("Running data quality checks")
    record_count = df.count()
    if record_count == 0:
        logger.error("Quality Check Failed: No records found after transformation!")
        raise ValueError("Empty DataFrame after transformation.")
    logger.info(f"Record count after transformation: {record_count}")

# Orchestrate Pipeline
def run_etl():
    logger.info("Starting E-Commerce Logistics ETL Pipeline")

    df_raw = load_raw_data()
    df_lookup = load_location_reference()
    df_transformed = transform_data(df_raw, df_lookup)
    df_enriched = enrich_with_audit_fields(df_transformed)

    run_quality_checks(df_enriched)

    logger.info("Sample output after transformation:")
    df_enriched.select("package_id", "delivery_efficiency", "cost_to_serve", "on_time").show(5)

    load_to_redshift(df_enriched)
    logger.info("ETL Pipeline Completed Successfully")

if __name__ == "__main__":
    run_etl()
