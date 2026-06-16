from pyspark.sql.functions import *

def validate_customer(customer_df,daily_order_df):

    validate_df = daily_order_df.join(customer_df,on="customer_id",how="left")

    validate_df = validate_df.withColumn("customer_reason",
                                        when(col("customer_name").isNull(),"INVALID_CUSTOMER")
                                        .otherwise(None))
    
    validate_df = validate_df.withColumn("amount_reason",
                                        when(col("order_amount") <= 0,"INVALID_AMOUNT")
                                        .otherwise(None))
    
    validate_df = validate_df.withColumn("quantity_reason",
                                        when(col("quantity") <= 0,"INVALID_QUANTITY")
                                        .otherwise(None))
    
    validate_df = validate_df.withColumn("status_reason",
                                        when(~col("order_status")
                                        .isin("DELIVERED","SHIPPED","PENDING","CANCELLED"),"INVALID_STATUS")
                                        .otherwise(None))
    
    validate_df = validate_df.withColumn("error_reason",
                                         concat_ws(",",col("customer_reason"),
                                                       col("amount_reason"),
                                                       col("quantity_reason"),
                                                       col("status_reason")
                                                  ) 
                                        )
    
    validate_df = validate_df.withColumn("error_reason",
                                        when(col("error_reason") == "" ,lit(None))
                                        .otherwise(col("error_reason"))
                                        )
    
    valid_df = validate_df.filter(col("error_reason").isNull())

    invalid_df = validate_df.filter(col("error_reason").isNotNull())

    return valid_df,invalid_df