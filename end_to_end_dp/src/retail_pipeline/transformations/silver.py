from pyspark.sql import functions as F
from pyspark import pipelines as dp

# Customers table

customers_rule = {"valid_city": "city IS NOT NULL"}

@dp.temporary_view
@dp.expect_or_drop("valid_id", "customer_id IS NOT NULL")
@dp.expect_all(customers_rule)
def customers_vw():
  
  df = (
      spark
      .readStream
      .table(("sdp_data_pipeline.bronze.customers_bronze"))
      .withColumn("timestamp", F.col("timestamp").cast("timestamp"))
      .withWatermark("timestamp", "1 hours")
      .dropDuplicates()
      .withColumn("first_name", F.sha2(F.col("first_name"), 256))
      .withColumn("last_name", F.sha2(F.col("last_name"), 256))
      .withColumn("address", F.sha2(F.col("city"), 256))
      .withColumn("email", F.sha2(F.col("email"), 256))
      .withColumn("loyalty_tier", F.trim("loyalty_tier"))
  )

  return df

dp.create_streaming_table("sdp_data_pipeline.silver.customers_silver")

dp.create_auto_cdc_flow(
    target = "sdp_data_pipeline.silver.customers_silver",
    source = "customers_vw",
    keys = ["customer_id"],
    sequence_by = "timestamp",
    except_column_list = ["operation"],
    stored_as_scd_type = 2
)

# Orders table

orders_rule = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_qty": "qty > 0",
    "valid_unit_price": "unit_price > 0",
    "valid_discount_pct": "discount_pct > 0",
    "valid_total_amount": "total_amount > 0"
}

@dp.table(
    name = "sdp_data_pipeline.silver.orders_silver"
)
@dp.expect_all_or_drop(orders_rule)
def orders_silver():

    df = (
        spark
        .readStream
        .table("sdp_data_pipeline.bronze.orders_bronze")
        .withColumn("order_timestamp", F.col("order_timestamp").cast("timestamp"))
        .withWatermark("order_timestamp", "1 hours")
        .dropDuplicates(["order_id"])
        .withColumn("category", F.trim("category"))
        .withColumn("channel", F.trim("channel"))
        .withColumn("city", F.trim("city"))
        .withColumn("country", F.trim("country"))
        .withColumn("customer_id", F.trim("customer_id"))
        .withColumn("region", F.trim("region"))
        .withColumn("sku", F.trim("sku"))
        .withColumn("qty", F.col("qty").cast("integer"))
        .withColumn("order_date", F.col("order_date").cast("date"))
        .withColumn("total_amount", F.col("total_amount").cast("decimal(12, 2)"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(12, 2)"))
    )

    return df

