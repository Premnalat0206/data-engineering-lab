from pyspark.sql.functions import *
from pyspark.sql.window import *

def transformed_data(valid_df,processed_df,customer_df):

    incremental_df = valid_df.join(processed_df,on="transaction_id",how="left_anti")

    incremental_df = incremental_df.join(customer_df,on="customer_id",how="left")

    incremental_df = incremental_df.withColumn("transaction_date",to_date(col("transaction_date")))

    incremental_df = incremental_df.withColumn("transaction_direction",
                                               when(col("transaction_type") =="CREDIT","Money_In")\
                                               .otherwise("Money_Out")
                                               )
    
    incremental_window = Window.partitionBy("customer_id").orderBy("transaction_date")

    incremental_df = incremental_df.withColumn("previous_transaction_amount",
                                               lag("amount",1).over(incremental_window)
                                               )
    
    return incremental_df


