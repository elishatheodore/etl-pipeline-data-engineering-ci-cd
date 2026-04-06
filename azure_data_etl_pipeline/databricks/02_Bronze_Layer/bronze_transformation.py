# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Transformation
# MAGIC 
# MAGIC This notebook transforms raw CSV data into the Bronze layer using Delta Lake format.
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Azure Data Lake (Raw CSV) → Databricks Spark → Azure Data Lake (Bronze Delta)
# MAGIC ```

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
import logging
from datetime import datetime

# COMMAND ----------

# Configure logging
logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

# COMMAND ----------

# Get parameters from ADF
dbutils.widgets.text("source_path", "raw")
dbutils.widgets.text("bronze_path", "bronze")
dbutils.widgets.text("storage_account", "stbrazilianecommerce")

source_path = dbutils.widgets.get("source_path")
bronze_path = dbutils.widgets.get("bronze_path")
storage_account = dbutils.widgets.get("storage_account")

# Construct full paths
base_path = f"abfss://{source_path}@{storage_account}.dfs.core.windows.net"
bronze_base_path = f"abfss://{bronze_path}@{storage_account}.dfs.core.windows.net"

logger.info(f"Source path: {base_path}")
logger.info(f"Bronze path: {bronze_base_path}")

# COMMAND ----------

# Define schema for each table
schema_dict = {
    "customers": StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_unique_id", StringType(), True),
        StructField("customer_zip_code_prefix", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True)
    ]),
    "geolocation": StructType([
        StructField("geolocation_zip_code_prefix", StringType(), True),
        StructField("geolocation_lat", DoubleType(), True),
        StructField("geolocation_lng", DoubleType(), True),
        StructField("geolocation_city", StringType(), True),
        StructField("geolocation_state", StringType(), True)
    ]),
    "order_items": StructType([
        StructField("order_id", StringType(), True),
        StructField("order_item_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("seller_id", StringType(), True),
        StructField("shipping_limit_date", TimestampType(), True),
        StructField("price", DoubleType(), True),
        StructField("freight_value", DoubleType(), True)
    ]),
    "order_payments": StructType([
        StructField("order_id", StringType(), True),
        StructField("payment_sequential", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("payment_installments", IntegerType(), True),
        StructField("payment_value", DoubleType(), True)
    ]),
    "order_reviews": StructType([
        StructField("review_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("review_score", IntegerType(), True),
        StructField("review_comment_title", StringType(), True),
        StructField("review_comment_message", StringType(), True),
        StructField("review_creation_date", TimestampType(), True),
        StructField("review_answer_timestamp", TimestampType(), True)
    ]),
    "orders": StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_approved_at", TimestampType(), True),
        StructField("order_delivered_carrier_date", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
        StructField("order_estimated_delivery_date", TimestampType(), True)
    ]),
    "products": StructType([
        StructField("product_id", StringType(), True),
        StructField("product_category_name", StringType(), True),
        StructField("product_name_lenght", IntegerType(), True),
        StructField("product_description_lenght", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("product_weight_g", DoubleType(), True),
        StructField("product_length_cm", DoubleType(), True),
        StructField("product_height_cm", DoubleType(), True),
        StructField("product_width_cm", DoubleType(), True)
    ]),
    "sellers": StructType([
        StructField("seller_id", StringType(), True),
        StructField("seller_zip_code_prefix", StringType(), True),
        StructField("seller_city", StringType(), True),
        StructField("seller_state", StringType(), True)
    ]),
    "product_category_translation": StructType([
        StructField("product_category_name", StringType(), True),
        StructField("product_category_name_english", StringType(), True)
    ])
}

# COMMAND ----------

# File mapping
file_mapping = {
    "olist_customers_dataset.csv": "customers",
    "olist_geolocation_dataset.csv": "geolocation",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_orders_dataset.csv": "orders",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "product_category_name_translation.csv": "product_category_translation"
}

# COMMAND ----------

class BronzeLayerTransformer:
    """Transform raw CSV data into Bronze Delta tables"""
    
    def __init__(self, source_path: str, bronze_path: str):
        self.source_path = source_path
        self.bronze_path = bronze_path
        self.processing_log = []
    
    def read_csv_with_schema(self, file_name: str, table_name: str):
        """Read CSV file with predefined schema"""
        try:
            file_path = f"{self.source_path}/{file_name}"
            schema = schema_dict[table_name]
            
            df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .schema(schema) \
                .load(file_path)
            
            logger.info(f"Successfully read {file_name} with {df.count()} rows")
            return df
            
        except Exception as e:
            logger.error(f"Error reading {file_name}: {str(e)}")
            raise
    
    def add_metadata_columns(self, df: DataFrame, table_name: str):
        """Add metadata columns for tracking"""
        return df.withColumn("bronze_ingestion_timestamp", current_timestamp()) \
                .withColumn("bronze_source_file", lit(table_name)) \
                .withColumn("bronze_batch_id", lit(uuid())) \
                .withColumn("bronze_processed_date", current_date())
    
    def handle_data_quality(self, df: DataFrame, table_name: str):
        """Apply data quality checks and transformations"""
        
        # Remove exact duplicates
        initial_count = df.count()
        df = df.dropDuplicates()
        final_count = df.count()
        
        if initial_count != final_count:
            logger.info(f"Removed {initial_count - final_count} duplicate rows from {table_name}")
        
        # Handle null values based on table
        if table_name == "customers":
            df = df.filter(col("customer_id").isNotNull() & col("customer_unique_id").isNotNull())
        elif table_name == "orders":
            df = df.filter(col("order_id").isNotNull() & col("customer_id").isNotNull())
        elif table_name == "products":
            df = df.filter(col("product_id").isNotNull())
        elif table_name == "sellers":
            df = df.filter(col("seller_id").isNotNull())
        
        return df
    
    def write_delta_table(self, df: DataFrame, table_name: str):
        """Write DataFrame as Delta table"""
        try:
            table_path = f"{self.bronze_path}/{table_name}"
            
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy("bronze_processed_date") \
                .save(table_path)
            
            logger.info(f"Successfully wrote {table_name} to Delta table at {table_path}")
            
            # Register table in metastore
            spark.sql(f"DROP TABLE IF EXISTS bronze_{table_name}")
            spark.sql(f"""
                CREATE TABLE bronze_{table_name}
                USING DELTA
                LOCATION '{table_path}'
            """)
            
            logger.info(f"Registered bronze_{table_name} in metastore")
            
        except Exception as e:
            logger.error(f"Error writing {table_name} to Delta: {str(e)}")
            raise
    
    def process_table(self, file_name: str, table_name: str):
        """Process a single table"""
        try:
            logger.info(f"Processing {table_name} from {file_name}")
            
            # Read CSV with schema
            df = self.read_csv_with_schema(file_name, table_name)
            
            # Add metadata
            df = self.add_metadata_columns(df, table_name)
            
            # Apply data quality
            df = self.handle_data_quality(df, table_name)
            
            # Write to Delta
            self.write_delta_table(df, table_name)
            
            # Log processing info
            processing_info = {
                "table": table_name,
                "file": file_name,
                "rows_processed": df.count(),
                "columns": len(df.columns),
                "status": "success",
                "timestamp": datetime.now().isoformat()
            }
            self.processing_log.append(processing_info)
            
            logger.info(f"Successfully processed {table_name}")
            
        except Exception as e:
            processing_info = {
                "table": table_name,
                "file": file_name,
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            self.processing_log.append(processing_info)
            logger.error(f"Failed to process {table_name}: {str(e)}")
            raise
    
    def process_all_tables(self):
        """Process all tables"""
        logger.info("Starting Bronze layer transformation")
        
        for file_name, table_name in file_mapping.items():
            self.process_table(file_name, table_name)
        
        logger.info("Bronze layer transformation completed")
    
    def generate_processing_report(self):
        """Generate processing report"""
        successful_tables = [log for log in self.processing_log if log["status"] == "success"]
        failed_tables = [log for log in self.processing_log if log["status"] == "failed"]
        
        report = {
            "summary": {
                "total_tables": len(file_mapping),
                "successful": len(successful_tables),
                "failed": len(failed_tables),
                "total_rows_processed": sum(log.get("rows_processed", 0) for log in successful_tables)
            },
            "successful_tables": successful_tables,
            "failed_tables": failed_tables,
            "timestamp": datetime.now().isoformat()
        }
        
        return report

# COMMAND ----------

# Import uuid for batch ID generation
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

# Add uuid function
uuid_udf = udf(lambda: str(uuid.uuid4()), StringType())

# COMMAND ----------

# Main execution
def main():
    """Main execution function"""
    logger.info("Starting Bronze layer transformation")
    
    try:
        transformer = BronzeLayerTransformer(base_path, bronze_base_path)
        transformer.process_all_tables()
        
        # Generate and log report
        report = transformer.generate_processing_report()
        logger.info(f"Bronze transformation report: {json.dumps(report, indent=2)}")
        
        # Display summary
        print("=== Bronze Layer Transformation Summary ===")
        print(f"Total tables processed: {report['summary']['total_tables']}")
        print(f"Successful: {report['summary']['successful']}")
        print(f"Failed: {report['summary']['failed']}")
        print(f"Total rows processed: {report['summary']['total_rows_processed']:,}")
        
        return report
        
    except Exception as e:
        logger.error(f"Bronze layer transformation failed: {str(e)}")
        raise

# COMMAND ----------

# Execute main function
if __name__ == "__main__":
    result = main()
    print(f"Bronze transformation completed: {json.dumps(result, indent=2)}")
