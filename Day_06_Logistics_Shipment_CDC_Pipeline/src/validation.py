from pyspark.sql.functions import *

def validate_shipment_data(customer_df,daily_shipments_df):

    validate_df = daily_shipments_df.join(customer_df,on="customer_id",how="left")

    validate_df = validate_df.withColumn("customer_reason",
                                         when(col("customer_name").isNull(),"INVALID_CUSTOMER")
                                        .otherwise(None)
                                        )

    validate_df = validate_df.withColumn("cost_reason",
                                         when(col("shipment_cost") <= 0 , "INVALID_COST")
                                         .otherwise(None)
                                        )
    
    validate_df = validate_df.withColumn("shipment_reason",
                                         when(~col("shipment_status").isin(["IN_TRANSIT","DELIVERED","PENDING"]),"INVALID_STATUS")
                                         .otherwise(None)
                                         )
    
    validate_df = validate_df.withColumn("error_reason",
                                         concat_ws(",",
                                                   col("customer_reason"),
                                                   col("cost_reason"),
                                                   col("shipment_reason")
                                                   )
                                         )
    
    valid_df = validate_df.filter(col("error_reason").isNull() | (col("error_reason") == ""))

    invalid_df = validate_df.filter(col("error_reason") != "")

    valid_df = valid_df.select(daily_shipments_df.columns)

    return valid_df,invalid_df