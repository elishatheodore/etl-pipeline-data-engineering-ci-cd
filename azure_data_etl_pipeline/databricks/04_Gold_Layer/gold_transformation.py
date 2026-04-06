# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Transformation
# MAGIC 
# MAGIC This notebook creates dimensional model (star schema) from Silver layer data for analytics.
# MAGIC 
# MAGIC ## Architecture
# MAGIC ```
# MAGIC Azure Data Lake (Silver Delta) → Databricks Spark → Azure Data Lake (Gold Delta)
# MAGIC ```
# MAGIC 
# MAGIC ## Star Schema
# MAGIC ```
# MAGIC fact_orders (central fact table)
# ├── dim_customers
# ├── dim_products  
# ├── dim_sellers
# ├── dim_date
# ├── dim_payment_type
# └── dim_review_sentiment
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
dbutils.widgets.text("silver_path", "silver")
dbutils.widgets.text("gold_path", "gold")
dbutils.widgets.text("storage_account", "stbrazilianecommerce")

silver_path = dbutils.widgets.get("silver_path")
gold_path = dbutils.widgets.get("gold_path")
storage_account = dbutils.widgets.get("storage_account")

# Construct full paths
silver_base_path = f"abfss://{silver_path}@{storage_account}.dfs.core.windows.net"
gold_base_path = f"abfss://{gold_path}@{storage_account}.dfs.core.windows.net"

logger.info(f"Silver path: {silver_base_path}")
logger.info(f"Gold path: {gold_base_path}")

# COMMAND ----------

class GoldLayerTransformer:
    """Transform Silver layer data into Gold dimensional model"""
    
    def __init__(self, silver_path: str, gold_path: str):
        self.silver_path = silver_path
        self.gold_path = gold_path
        self.processing_log = []
    
    def read_silver_table(self, table_name: str):
        """Read Silver Delta table"""
        try:
            table_path = f"{self.silver_path}/{table_name}"
            df = spark.read.format("delta").load(table_path)
            logger.info(f"Successfully read silver_{table_name} with {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading silver_{table_name}: {str(e)}")
            raise
    
    def create_dim_customers(self):
        """Create customer dimension table"""
        logger.info("Creating dim_customers")
        
        customers_df = self.read_silver_table("customers")
        
        # Create customer dimension with surrogate key
        dim_customers = customers_df.select(
            col("customer_id"),
            col("customer_unique_id"),
            col("customer_zip_code_prefix"),
            col("customer_zip_code_prefix_clean"),
            col("customer_city"),
            col("customer_state"),
            col("customer_region")
        ).distinct()
        
        # Add surrogate key
        window = Window.orderBy("customer_id")
        dim_customers = dim_customers.withColumn("customer_key", row_number().over(window))
        
        # Add metadata
        dim_customers = dim_customers.withColumn("gold_ingestion_timestamp", current_timestamp()) \
                                     .withColumn("gold_batch_id", lit(uuid()))
        
        return dim_customers
    
    def create_dim_products(self):
        """Create product dimension table"""
        logger.info("Creating dim_products")
        
        products_df = self.read_silver_table("products")
        
        # Create product dimension with surrogate key
        dim_products = products_df.select(
            col("product_id"),
            col("product_category_name"),
            col("product_category_name_english"),
            col("product_name_lenght"),
            col("product_description_lenght"),
            col("product_photos_qty"),
            col("product_weight_g"),
            col("product_length_cm"),
            col("product_height_cm"),
            col("product_width_cm"),
            col("product_volume_cm3"),
            col("product_density_g_per_cm3")
        ).distinct()
        
        # Add surrogate key
        window = Window.orderBy("product_id")
        dim_products = dim_products.withColumn("product_key", row_number().over(window))
        
        # Add metadata
        dim_products = dim_products.withColumn("gold_ingestion_timestamp", current_timestamp()) \
                                   .withColumn("gold_batch_id", lit(uuid()))
        
        return dim_products
    
    def create_dim_sellers(self):
        """Create seller dimension table"""
        logger.info("Creating dim_sellers")
        
        sellers_df = self.read_silver_table("sellers")
        
        # Create seller dimension with surrogate key
        dim_sellers = sellers_df.select(
            col("seller_id"),
            col("seller_zip_code_prefix"),
            col("seller_zip_code_prefix_clean"),
            col("seller_city"),
            col("seller_state"),
            col("seller_region")
        ).distinct()
        
        # Add surrogate key
        window = Window.orderBy("seller_id")
        dim_sellers = dim_sellers.withColumn("seller_key", row_number().over(window))
        
        # Add metadata
        dim_sellers = dim_sellers.withColumn("gold_ingestion_timestamp", current_timestamp()) \
                                 .withColumn("gold_batch_id", lit(uuid()))
        
        return dim_sellers
    
    def create_dim_date(self):
        """Create date dimension table"""
        logger.info("Creating dim_date")
        
        # Get date range from orders
        orders_df = self.read_silver_table("orders")
        min_date = orders_df.agg(min("order_purchase_date")).collect()[0][0]
        max_date = orders_df.agg(max("order_purchase_date")).collect()[0][0]
        
        # Generate date dimension
        start_date = min_date if min_date else datetime(2016, 1, 1).date()
        end_date = max_date if max_date else datetime(2018, 12, 31).date()
        
        date_list = []
        current_date = start_date
        while current_date <= end_date:
            date_list.append((current_date,))
            current_date += timedelta(days=1)
        
        # Create DataFrame
        date_df = spark.createDataFrame(date_list, ["date"])
        
        # Add date attributes
        dim_date = date_df.withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast(IntegerType())) \
                          .withColumn("year", year(col("date"))) \
                          .withColumn("quarter", quarter(col("date"))) \
                          .withColumn("month", month(col("date"))) \
                          .withColumn("month_name", date_format(col("date"), "MMMM"))) \
                          .withColumn("day", dayofmonth(col("date"))) \
                          .withColumn("day_of_week", dayofweek(col("date"))) \
                          .withColumn("day_name", date_format(col("date"), "EEEE"))) \
                          .withColumn("day_of_year", dayofyear(col("date"))) \
                          .withColumn("week_of_year", weekofyear(col("date"))) \
                          .withColumn("is_weekend", when(dayofweek(col("date")).isin([1, 7]), lit(True)).otherwise(lit(False))) \
                          .withColumn("is_holiday", lit(False))  # Could be enhanced with actual Brazilian holidays
        
        # Add metadata
        dim_date = dim_date.withColumn("gold_ingestion_timestamp", current_timestamp()) \
                           .withColumn("gold_batch_id", lit(uuid()))
        
        return dim_date
    
    def create_dim_payment_type(self):
        """Create payment type dimension table"""
        logger.info("Creating dim_payment_type")
        
        payments_df = self.read_silver_table("order_payments")
        
        # Create payment type dimension
        dim_payment_type = payments_df.select(
            col("payment_type"),
            col("payment_type_clean")
        ).distinct()
        
        # Add surrogate key
        window = Window.orderBy("payment_type")
        dim_payment_type = dim_payment_type.withColumn("payment_type_key", row_number().over(window))
        
        # Add metadata
        dim_payment_type = dim_payment_type.withColumn("gold_ingestion_timestamp", current_timestamp()) \
                                          .withColumn("gold_batch_id", lit(uuid()))
        
        return dim_payment_type
    
    def create_dim_review_sentiment(self):
        """Create review sentiment dimension table"""
        logger.info("Creating dim_review_sentiment")
        
        reviews_df = self.read_silver_table("order_reviews")
        
        # Create review sentiment dimension
        dim_review_sentiment = reviews_df.select(
            col("review_score"),
            col("review_sentiment")
        ).distinct()
        
        # Add surrogate key
        window = Window.orderBy("review_score")
        dim_review_sentiment = dim_review_sentiment.withColumn("review_sentiment_key", row_number().over(window))
        
        # Add metadata
        dim_review_sentiment = dim_review_sentiment.withColumn("gold_ingestion_timestamp", current_timestamp()) \
                                                 .withColumn("gold_batch_id", lit(uuid()))
        
        return dim_review_sentiment
    
    def create_fact_orders(self):
        """Create orders fact table"""
        logger.info("Creating fact_orders")
        
        # Read all required silver tables
        orders_df = self.read_silver_table("orders")
        order_items_df = self.read_silver_table("order_items")
        order_payments_df = self.read_silver_table("order_payments")
        order_reviews_df = self.read_silver_table("order_reviews")
        
        # Join order items with orders
        fact_orders = order_items_df.join(orders_df, "order_id", "inner")
        
        # Join with payments (aggregate to handle multiple payment methods)
        payment_agg = order_payments_df.groupBy("order_id") \
                                     .agg(
                                         sum("payment_value").alias("total_payment_value"),
                                         count("payment_sequential").alias("payment_methods_count"),
                                         first("payment_type").alias("primary_payment_type")
                                     )
        
        fact_orders = fact_orders.join(payment_agg, "order_id", "left")
        
        # Join with reviews (get the latest review)
        review_window = Window.partitionBy("order_id").orderBy(col("review_creation_date").desc())
        latest_reviews = order_reviews_df.withColumn("rn", row_number().over(review_window)) \
                                         .filter(col("rn") == 1) \
                                         .drop("rn")
        
        fact_orders = fact_orders.join(latest_reviews, "order_id", "left")
        
        # Add date key
        fact_orders = fact_orders.withColumn("date_key", 
                                            date_format(col("order_purchase_date"), "yyyyMMdd").cast(IntegerType()))
        
        # Add calculated measures
        fact_orders = fact_orders.withColumn("order_item_total_value", col("price") + col("freight_value")) \
                                 .withColumn("freight_percentage", 
                                           when(col("price") > 0, col("freight_value") / col("price")).otherwise(0))
        
        # Select final fact table columns
        fact_orders = fact_orders.select(
            col("order_id"),
            col("customer_id"),
            col("product_id"),
            col("seller_id"),
            col("date_key"),
            col("order_item_id"),
            col("price"),
            col("freight_value"),
            col("order_item_total_value"),
            col("freight_percentage"),
            col("total_payment_value"),
            col("payment_methods_count"),
            col("primary_payment_type"),
            col("review_score"),
            col("review_sentiment"),
            col("order_status"),
            col("delivery_status"),
            col("order_delivery_days"),
            col("order_estimated_delivery_days"),
            col("order_delivery_delay_days"),
            col("order_purchase_timestamp"),
            col("order_approved_at"),
            col("order_delivered_carrier_date"),
            col("order_delivered_customer_date"),
            col("order_estimated_delivery_date")
        )
        
        # Add metadata
        fact_orders = fact_orders.withColumn("gold_ingestion_timestamp", current_timestamp()) \
                                 .withColumn("gold_batch_id", lit(uuid()))
        
        return fact_orders
    
    def write_delta_table(self, df: DataFrame, table_name: str, partition_cols=None):
        """Write DataFrame as Delta table"""
        try:
            table_path = f"{self.gold_path}/{table_name}"
            
            writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
            
            if partition_cols:
                writer = writer.partitionBy(partition_cols)
            
            writer.save(table_path)
            
            logger.info(f"Successfully wrote gold_{table_name} to Delta table at {table_path}")
            
            # Register table in metastore
            spark.sql(f"DROP TABLE IF EXISTS gold_{table_name}")
            spark.sql(f"""
                CREATE TABLE gold_{table_name}
                USING DELTA
                LOCATION '{table_path}'
            """)
            
            logger.info(f"Registered gold_{table_name} in metastore")
            
        except Exception as e:
            logger.error(f"Error writing gold_{table_name} to Delta: {str(e)}")
            raise
    
    def process_all_tables(self):
        """Process all dimensional model tables"""
        logger.info("Starting Gold layer transformation")
        
        # Create dimension tables
        dimensions = {
            "dim_customers": self.create_dim_customers(),
            "dim_products": self.create_dim_products(),
            "dim_sellers": self.create_dim_sellers(),
            "dim_date": self.create_dim_date(),
            "dim_payment_type": self.create_dim_payment_type(),
            "dim_review_sentiment": self.create_dim_review_sentiment()
        }
        
        # Write dimension tables
        for dim_name, dim_df in dimensions.items():
            try:
                self.write_delta_table(dim_df, dim_name)
                self.processing_log.append({
                    "table": dim_name,
                    "rows_processed": dim_df.count(),
                    "columns": len(dim_df.columns),
                    "status": "success",
                    "timestamp": datetime.now().isoformat()
                })
                logger.info(f"Successfully processed {dim_name}")
            except Exception as e:
                self.processing_log.append({
                    "table": dim_name,
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                })
                logger.error(f"Failed to process {dim_name}: {str(e)}")
        
        # Create and write fact table
        try:
            fact_df = self.create_fact_orders()
            self.write_delta_table(fact_df, "fact_orders", partition_cols=["date_key"])
            self.processing_log.append({
                "table": "fact_orders",
                "rows_processed": fact_df.count(),
                "columns": len(fact_df.columns),
                "status": "success",
                "timestamp": datetime.now().isoformat()
            })
            logger.info("Successfully processed fact_orders")
        except Exception as e:
            self.processing_log.append({
                "table": "fact_orders",
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
            logger.error(f"Failed to process fact_orders: {str(e)}")
        
        logger.info("Gold layer transformation completed")
    
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
    logger.info("Starting Gold layer transformation")
    
    try:
        transformer = GoldLayerTransformer(silver_base_path, gold_base_path)
        transformer.process_all_tables()
        
        # Generate and log report
        report = transformer.generate_processing_report()
        logger.info(f"Gold transformation report: {json.dumps(report, indent=2)}")
        
        # Display summary
        print("=== Gold Layer Transformation Summary ===")
        print(f"Total tables processed: {report['summary']['total_tables']}")
        print(f"Successful: {report['summary']['successful']}")
        print(f"Failed: {report['summary']['failed']}")
        print(f"Total rows processed: {report['summary']['total_rows_processed']:,}")
        
        # Display star schema info
        print("\n=== Star Schema Created ===")
        print("Dimension Tables:")
        print("- dim_customers")
        print("- dim_products")
        print("- dim_sellers")
        print("- dim_date")
        print("- dim_payment_type")
        print("- dim_review_sentiment")
        print("\nFact Table:")
        print("- fact_orders")
        
        return report
        
    except Exception as e:
        logger.error(f"Gold layer transformation failed: {str(e)}")
        raise

# COMMAND ----------

# Execute main function
if __name__ == "__main__":
    result = main()
    print(f"Gold transformation completed: {json.dumps(result, indent=2)}")
