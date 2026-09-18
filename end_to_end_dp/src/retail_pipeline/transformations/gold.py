from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.materialized_view(
    name="sdp_data_pipeline.gold.dim_customer",
    comment="Dimension Customer - Modélisation Kimball"
)
def dim_customer():

    df = (
        spark.read
        .table("sdp_data_pipeline.silver.orders_silver")
        .select(
            F.col("customer_id"),
            F.col("region"),
            F.col("country"),
            F.col("city")
        )
        .dropDuplicates(["customer_id"])
    )
    
    return df

@dp.materialized_view(
    name="sdp_data_pipeline.gold.dim_product",
    comment="Dimension Product - Modélisation Kimball"
)
def dim_product():

    df = (
        spark.read
        .table("sdp_data_pipeline.silver.orders_silver")
        .select(
            F.col("sku"),
            F.col("category")
        )
        .dropDuplicates(["sku"])
        .withColumn("product_key", F.monotonically_increasing_id())
        .select(
            "product_key",
            "sku",
            "category"
        )
    )
    
    return df

@dp.materialized_view(
    name="sdp_data_pipeline.gold.dim_date",
    comment="Dimension Date - Modélisation Kimball"
)
def dim_date():

    df = (
        spark.read
        .table("sdp_data_pipeline.silver.orders_silver")
        .select(F.col("order_date").alias("date"))
        .distinct()
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("integer"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("is_weekend", F.when(F.dayofweek("date").isin([1, 7]), True).otherwise(False))
        .select(
            "date_key",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_of_week",
            "day_name",
            "week_of_year",
            "is_weekend"
        )
    )
    
    return df

@dp.materialized_view(
    name="sdp_data_pipeline.gold.dim_channel",
    comment="Dimension Channel - Modélisation Kimball"
)
def dim_channel():
    """
    Dimension Channel (canal de vente)
    """
    df = (
        spark.read
        .table("sdp_data_pipeline.silver.orders_silver")
        .select(F.col("channel"))
        .distinct()
        .withColumn("channel_key", F.monotonically_increasing_id())
        .select(
            "channel_key",
            "channel"
        )
    )
    
    return df

@dp.materialized_view(
    name="sdp_data_pipeline.gold.fact_orders",
    comment="Table de faits Orders - Modélisation en étoile Kimball"
)
def fact_orders():

    orders = spark.read.table("sdp_data_pipeline.silver.orders_silver")
    
    
    dim_prod = spark.read.table("sdp_data_pipeline.gold.dim_product")
    dim_chan = spark.read.table("sdp_data_pipeline.gold.dim_channel")
    
    df = (
        orders
        .join(
            dim_prod.select("product_key", "sku"),
            "sku",
            "left"
        )
        .join(
            dim_chan.select("channel_key", "channel"),
            "channel",
            "left"
        )
        .withColumn("date_key", F.date_format("order_date", "yyyyMMdd").cast("integer"))
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("product_key"),
            F.col("date_key"),
            F.col("channel_key"),
            F.col("order_timestamp"),
            F.col("qty"),
            F.col("unit_price"),
            F.col("discount_pct"),
            F.coalesce(F.col("coupon_code"), F.lit("")).alias("coupon_code"),
            F.col("total_amount")
        )
    )
    
    return df