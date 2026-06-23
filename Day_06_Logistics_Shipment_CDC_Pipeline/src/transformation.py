from pyspark.sql.functions import *
from pyspark.sql.window import *

def find_new_shipments(valid_df,existing_df):

    new_shipments_df = valid_df.join(existing_df,on="shipment_id",how="left_anti")
    
    return new_shipments_df

def find_updated_shipments(valid_df,existing_df):

    updated_shipments_df = valid_df.alias("v").join(existing_df.alias("e"),
                                                    on="shipment_id",
                                                    how="inner")

    updated_shipments_df = updated_shipments_df.filter((col("v.shipment_status") != col("e.shipment_status"))
                                                       |
                                                       (col("v.shipment_cost")   != col("e.shipment_cost"))
                                                       )
    
    updated_shipments_df = updated_shipments_df.select("v.*")

    return updated_shipments_df

def find_deleted_shipments(valid_df,existing_df):

    deleted_shipments_df = existing_df.join(valid_df,on="shipment_id",how="left_anti")


    return deleted_shipments_df

def transform_shipment_data(valid_df,customer_df):

    transformed_df = valid_df.join(broadcast(customer_df),on="customer_id",how="left")


    transformed_df = transformed_df.withColumn("shipment_date",to_date(col("shipment_date")))\
                                   .withColumn("expected_delivery_date",to_date(col("expected_delivery_date")))
    
    transformed_df = transformed_df.withColumn("delivery_gap_days",datediff("expected_delivery_date","shipment_date"))

    transformed_window = Window.partitionBy("customer_id").orderBy("shipment_date")

    transformed_df = transformed_df.withColumn("next_shipment_cost",lead("shipment_cost").over(transformed_window))

    return transformed_df
