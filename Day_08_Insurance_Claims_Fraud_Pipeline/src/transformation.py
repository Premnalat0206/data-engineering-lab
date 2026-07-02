from pyspark.sql.functions import *
from pyspark.sql.window import Window

def transform_data(valid_df, processed_df, customer_df, policy_df):

    watermark_df = processed_df.agg(max(col("claim_date")).alias("watermark_date"))

    row = watermark_df.first()

    watermark_date = row["watermark_date"]

    incremental_df = valid_df.filter(col("claim_date") > watermark_date)

    incremental_df = incremental_df.withColumn("claim_size",when(col("claim_amount") <= 25000,"Small")
                                               .when((col("claim_amount")>=25001) & (col("claim_amount") <= 50000),"Medium")
                                               .otherwise("Large")
                                               )
    
    window_incremental = Window.partitionBy("customer_id").orderBy("claim_date")

    incremental_df = incremental_df.withColumn("cumulative_claim_amount",sum("claim_amount").over(window_incremental))

    incremental_df = incremental_df.withColumn("fraud_flag",when(col("cumulative_claim_amount")> 150000,"HIGH_RISK").otherwise("NORMAL"))

    return incremental_df