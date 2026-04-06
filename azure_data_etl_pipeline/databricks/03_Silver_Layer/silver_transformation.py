# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Transformation
# MAGIC 
# MAGIC This notebook transforms Bronze layer data into curated Silver layer with business logic applied.
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Azure Data Lake (Bronze Delta) → Databricks Spark → Azure Data Lake (Silver Delta)
# MAGIC ```

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
import logging
from datetime import datetime, timedelta

# COMMAND ----------

# Configure logging
logger = spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(__name__)

# COMMAND ----------

# Get parameters from ADF
dbutils.widgets.text("bronze_path", "bronze")
dbutils.widgets.text("silver_path", "silver")
dbutils.widgets.text("storage_account", "stbrazilianecommerce")

bronze_path = dbutils.widgets.get("bronze_path")
silver_path = dbutils.widgets.get("silver_path")
storage_account = dbutils.widgets.get("storage_account")

# Construct full paths
bronze_base_path = f"abfss://{bronze_path}@{storage_account}.dfs.core.windows.net"
silver_base_path = f"abfss://{silver_path}@{storage_account}.dfs.core.windows.net"

logger.info(f"Bronze path: {bronze_base_path}")
logger.info(f"Silver path: {silver_base_path}")

# COMMAND ----------

class SilverLayerTransformer:
    """Transform Bronze layer data into curated Silver layer"""
    
    def __init__(self, bronze_path: str, silver_path: str):
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.processing_log = []
    
    def read_bronze_table(self, table_name: str):
        """Read Bronze Delta table"""
        try:
            table_path = f"{self.bronze_path}/{table_name}"
            df = spark.read.format("delta").load(table_path)
            logger.info(f"Successfully read bronze_{table_name} with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading bronze_{table_name}: {str(e)}")
            raise
    
    def transform_customers(self):
        """Transform customers data"""
        df = self.read_bronze_table("customers")
        
        # Add derived columns
        df = df.withColumn("customer_zip_code_prefix_clean", 
                          regexp_replace(col("customer_zip_code_prefix"), "[^0-9]", "")) \
                .withColumn("customer_region", 
                           when(col("customer_state").isin("SP", "RJ", "ES", "MG"), "Southeast")
                           .when(col("customer_state").isin("RS", "SC", "PR"), "South")
                           .when(col("customer_state").isin("GO", "MT", "MS", "DF"), "Central-West")
                           .when(col("customer_state").isin("BA", "SE", "AL", "PE", "PB", "RN", "CE", "PI", "MA"), "Northeast")
                           .when(col("customer_state").isin("AM", "RR", "AP", "PA", "TO", "AC", "RO"), "North")
                           .otherwise("Unknown"))
        
        # Add metadata
        df = df.withColumn("silver_ingestion_timestamp", current_timestamp()) \
                .withColumn("silver_batch_id", lit(uuid()))
        
        return df
    
    def transform_products(self):
        """Transform products data"""
        df = self.read_bronze_table("products")
        
        # Join with category translation
        translation_df = self.read_bronze_table("product_category_translation")
        
        df = df.join(translation_df, 
                    df.product_category_name == translation_df.product_category_name,
                    "left")
        
        # Handle missing categories
        df = df.withColumn("product_category_name_english", 
                           coalesce(col("product_category_name_english"), 
                                   col("product_category_name"), 
                                   lit("Uncategorized")))
        
        # Add derived columns
        df = df.withColumn("product_volume_cm3", 
                          col("product_length_cm") * col("product_height_cm") * col("product_width_cm")) \
                .withColumn("product_density_g_per_cm3", 
                          when(col("product_volume_cm3") > 0, 
                               col("product_weight_g") / col("product_volume_cm3")).otherwise(0))
        
        # Add metadata
        df = df.withColumn("silver_ingestion_timestamp", current_timestamp()) \
                .withColumn("silver_batch_id", lit(uuid()))
        
        return df
    
    def transform_orders(self):
        """Transform orders data"""
        df = self.read_bronze_table("orders")
        
        # Add derived columns
        df = df.withColumn("order_purchase_date", to_date(col("order_purchase_timestamp"))) \
                .withColumn("order_purchase_month", month(col("order_purchase_timestamp"))) \
                .withColumn("order_purchase_year", year(col("order_purchase_timestamp"))) \
                .withColumn("order_purchase_quarter", quarter(col("order_purchase_timestamp")))
        
        # Calculate delivery metrics
        df = df.withColumn("order_delivery_days", 
                          datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
                .withColumn("order_estimated_delivery_days", 
                          datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))) \
                .withColumn("order_delivery_delay_days", 
                          datediff(col("order_delivered_customer_date"), col("order_estimated_delivery_date")))
        
        # Add delivery status
        df = df.withColumn("delivery_status",
                          when(col("order_status") == "delivered", 
                               when(col("order_delivery_delay_days") <= 0, "On Time")
                               .when(col("order_delivery_delay_days") <= 7, "Late (1-7 days)")
                               .when(col("order_delivery_delay_days") <= 30, "Late (8-30 days)")
                               .otherwise("Very Late (>30 days)"))
                          .otherwise(col("order_status")))
        
        # Add metadata
        df = df.withColumn("silver_ingestion_timestamp", current_timestamp()) \
                .withColumn("silver_batch_id", lit(uuid()))
        
        return df
    
    def transform_sellers(self):
        """Transform sellers data"""
        df = self.read_bronze_table("sellers")
        
        # Add derived columns
        df = df.withColumn("seller_zip_code_prefix_clean", 
                          regexp_replace(col("seller_zip_code_prefix"), "[^0-9]", "")) \
                .withColumn("seller_region", 
                           when(col("seller_state").isin("SP", "RJ", "ES", "MG"), "Southeast")
                           .when(col("seller_state").isin("RS", "SC", "PR"), "South")
                           .when(col("seller_state").isin("GO", "MT", "MS", "DF"), "Central-West")
                           .when(col("seller_state").isin("BA", "SE", "AL", "PE", "PB", "RN", "CE", "PI", "MA"), "Northeast")
                           .when(col("seller_state").isin("AM", "RR", "AP", "PA", "TO", "AC", "RO"), "North")
                           .otherwise("Unknown"))
        
        # Add metadata
        df = df.withColumn("silver_ingestion_timestamp", current_timestamp()) \
                .withColumn("silver_batch_id", lit(uuid()))
        
        return df
    
    def transform_order_items(self):
        """Transform order items data"""
        df = self.read_bronze_table("order_items")
        
        # Add derived columns
        df = df.withColumn("order_item_total_value", col("price") + col("freight_value")) \
                .withColumn("freight_percentage", 
                          when(col("price") > 0, col("freight_value") / col("price")).otherwise(0))
        
        # Add metadata
        df = df.withColumn("silver_ingestion_timestamp", current_timestamp()) \
                .withColumn("silver_batch_id", lit(uuid()))
        
        return df
    
    def transform_order_payments(self):
        """Transform order payments data"""
        df = self.read_bronze_table("order_payments")
        
        # Add derived columns
        df = df.withColumn("payment_type_clean", 
                          when(col("payment_type") == "credit_card", "Credit Card")
                           .when(col("payment_type") == "debit_card", "Debit Card")
                           .when(col("payment_type") == "boleto", "Boleto")
                           .when(col("payment_type") == "voucher", "Voucher")
                           .otherwise(col("payment_type")))
        
        # Add metadata
        df = df.withColumn("silver_ingestion_timestamp", current_timestamp()) \
                .withColumn("silver_batch_id", lit(uuid()))
        
        return df
    
    def transform_order_reviews(self):
        """Transform order reviews data"""
        df = self.read_bronze_table("order_reviews")
        
        # Add derived columns
        df = df.withColumn("review_sentiment",
                          when(col("review_score") >= 4, "Positive")
                           .when(col("review_score") == 3, "Neutral")
                           .otherwise("Negative")) \
                .withColumn("review_response_time_hours",
                          datediff(col("review_answer_timestamp"), col("review_creation_date")) * 24)
        
        # Add metadata
        df = df.withColumn("silver_ingestion_timestamp", current_timestamp()) \
                .withColumn("silver_batch_id", lit(uuid()))
        
        return df
    
    def write_delta_table(self, df: DataFrame, table_name: str):
        """Write DataFrame as Delta table"""
        try:
            table_path = f"{self.silver_path}/{table_name}"
            
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .partitionBy("silver_ingestion_timestamp") \
                .save(table_path)
            
            logger.info(f"Successfully wrote silver_{table_name} to Delta table at {table_path}")
            
            # Register table in metastore
            spark.sql(f"DROP TABLE IF EXISTS silver_{table_name}")
            spark.sql(f"""
                CREATE TABLE silver_{table_name}
                USING DELTA
                LOCATION '{table_path}'
            """)
            
            logger.info(f"Registered silver_{table_name} in metastore")
            
        except Exception as e:
            logger.error(f"Error writing silver_{table_name} to Delta: {str(e)}")
            raise
    
    def process_table(self, table_name: str, transform_function):
        """Process a single table"""
        try:
            logger.info(f"Processing silver_{table_name}")
            
            # Apply transformation
            df = transform_function()
            
            # Write to Delta
            self.write_delta_table(df, table_name)
            
            # Log processing info
            processing_info = {
                "table": table_name,
                "rows_processed": df.count(),
                "columns": len(df.columns),
                "status": "success",
                "timestamp": datetime.now().isoformat()
            }
            self.processing_log.append(processing_info)
            
            logger.info(f"Successfully processed silver_{table_name}")
            
        except Exception as e:
            processing_info = {
                "table": table_name,
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            self.processing_log.append(processing_info)
            logger.error(f"Failed to process silver_{table_name}: {str(e)}")
            raise
    
    def process_all_tables(self):
        """Process all tables"""
        logger.info("Starting Silver layer transformation")
        
        # Define transformations
        transformations = {
            "customers": self.transform_customers,
            "products": self.transform_products,
            "orders": self.transform_orders,
            "sellers": self.transform_sellers,
            "order_items": self.transform_order_items,
            "order_payments": self.transform_order_payments,
            "order_reviews": self.transform_order_reviews
        }
        
        for table_name, transform_func in transformations.items():
            self.process_table(table_name, transform_func)
        
        logger.info("Silver layer transformation completed")
    
    def generate_processing_report(self):
        """Generate processing report"""
        successful_tables = [log for log in self.processing_log if log["status"] == "success"]
        failed_tables = [log for log in self.processing_log if log["status"] == "failed"]
        
        report = {
            "summary": {
                "total_tables": len(self.processing_log),
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
    logger.info("Starting Silver layer transformation")
    
    try:
        transformer = SilverLayerTransformer(bronze_base_path, silver_base_path)
        transformer.process_all_tables()
        
        # Generate and log report
        report = transformer.generate_processing_report()
        logger.info(f"Silver transformation report: {json.dumps(report, indent=2)}")
        
        # Display summary
        print("=== Silver Layer Transformation Summary ===")
        print(f"Total tables processed: {report['summary']['total_tables']}")
        print(f"Successful: {report['summary']['successful']}")
        print(f"Failed: {report['summary']['failed']}")
        print(f"Total rows processed: {report['summary']['total_rows_processed']:,}")
        
        return report
        
    except Exception as e:
        logger.error(f"Silver layer transformation failed: {str(e)}")
        raise

# COMMAND ----------

# Execute main function
if __name__ == "__main__":
    result = main()
    print(f"Silver transformation completed: {json.dumps(result, indent=2)}")
