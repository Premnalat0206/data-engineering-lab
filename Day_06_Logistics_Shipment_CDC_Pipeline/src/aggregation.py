from pyspark.sql.functions import *
from pyspark.sql.window import *

def generate_reports(transform_df):

    region_wise_shipment_cost = transform_df.groupBy(col("region"))\
                                            .agg(sum(col("shipment_cost")).alias("total_shipment_cost"))
    
    region_wise_avg_shipement_cost = transform_df.groupBy(col("region"))\
                                                 .agg(avg(col("shipment_cost")).alias("avg_shipment_cost"))
    
    customer_tier_wise_shipment_cost = transform_df.groupBy(col("customer_tier"))\
                                                   .agg(sum(col("shipment_cost")).alias("total_cost"))
    
    shipment_status_wise_count = transform_df.groupBy(col("shipment_status"))\
                                             .agg(count("*").alias("shipment_count"))
    
    shipment_window = Window.partitionBy("region").orderBy(desc("shipment_cost"))

    most_expensive_shipment_by_region = transform_df.withColumn("shipment_rank",
                                                                row_number().over(shipment_window))
    
    filter_most_expensive_shipment = most_expensive_shipment_by_region.filter(col("shipment_rank") == 1).drop("shipment_rank")

    return (
        region_wise_shipment_cost,
        region_wise_avg_shipement_cost,
        customer_tier_wise_shipment_cost,
        shipment_status_wise_count,
        filter_most_expensive_shipment
    )