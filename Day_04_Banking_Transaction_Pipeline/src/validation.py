from pyspark.sql.functions import *
from pyspark.sql.window import *

def validate_transactions(transaction_df,customer_df):

    validate_df = transaction_df.join(customer_df,on="customer_id",how="left")

    validate_df = validate_df.withColumn("customer_error",
                                         when(col("customer_name").isNull(),"INVALID_CUSTOMER").otherwise(None)
                                        )
    
    validate_df = validate_df.withColumn("amount_error",
                                         when(col("amount") <= 0,"INVALID_AMOUNT").otherwise(None)
                                        )
    
    validate_df = validate_df.withColumn("channel_error",
                                         when(~col("channel").isin("UPI","ATM","MOBILE","BRANCH"),"INVALID_CHANNEL").otherwise(None)
                                        )
    
    validate_df = validate_df.withColumn("transaction_type_error",
                                         when(~col("transaction_type").isin("CREDIT","DEBIT"),"INVALID_TRANSACTION_TYPE").otherwise(None)
                                        )
    
    validate_df = validate_df.withColumn("error_reason",
                                        concat_ws(",",col("customer_error")
                                                  ,col("amount_error")
                                                  ,col("channel_error")
                                                  ,col("transaction_type_error")
                                                )
                                        )
    validate_df = validate_df.withColumn("error_reason",
                                         when(col("error_reason") == "",lit(None)).otherwise(col("error_reason"))
                                        )
    
    validate_df = validate_df.drop("customer_error","amount_error","channel_error","transaction_type_error","customer_name","account_type")

    valid_df = validate_df.filter(col("error_reason").isNull())

    invalid_df = validate_df.filter(col("error_reason").isNotNull())

    return valid_df,invalid_df