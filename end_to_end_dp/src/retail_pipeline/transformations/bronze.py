from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.table(
    name = "sdp_data_pipeline.bronze.customers_bronze",
    table_properties = {
        "delta.appendOnly": "true",
        "pipelines.reset.allowed": "false"
    }
)
def customers_bronze():

    df = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/databricks_simulated_retail_customer_data/v02/customer_changes_daily")
    )

    return df

dp.create_streaming_table(name="sdp_data_pipeline.bronze.orders_bronze")

@dp.append_flow(target = "sdp_data_pipeline.bronze.orders_bronze")
def append_bright_homer_orders():

    df = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("/Volumes/databricks_simulated_retail_customer_data/v02/subsidiary_daily_orders/bright_home_orders/")
    )
    return df

@dp.append_flow(target = "sdp_data_pipeline.bronze.orders_bronze")
def append_lumina_sports_orders():

    df = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("/Volumes/databricks_simulated_retail_customer_data/v02/subsidiary_daily_orders/lumina_sports_orders/")
    )

    return df

@dp.append_flow(target = "sdp_data_pipeline.bronze.orders_bronze")
def append_northstar_outfitters_orders():

    df = (
        spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/Volumes/databricks_simulated_retail_customer_data/v02/subsidiary_daily_orders/northstar_outfitters_orders/")
    )

    return df