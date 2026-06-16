from pyspark.sql.functions import *

def generate_reports(incremental_df):

    channel_wise_df = incremental_df.groupBy("channel")\
                                           .agg(
                                               sum(col("amount")).alias("total_amount")
                                           )
    
    account_type_df = incremental_df.groupBy("account_type")\
                                           .agg(
                                               sum(col("amount")).alias("total_amount")
                                           )
    
    customer_avg_df = incremental_df.groupBy("customer_id")\
                                           .agg(
                                               avg(col("amount")).alias("average_amount")
                                           )
    
    transaction_type_df = incremental_df.groupBy("transaction_type")\
                                        .agg(
                                            sum(col("amount")).alias("total_amount")
                                        ) 
    
    top5_customer_df = incremental_df.groupBy("customer_id")\
                                      .agg(
                                          sum(col("amount")).alias("total_amount")
                                          )\
                                       .orderBy(
                                              desc(col("total_amount"))
                                              )\
                                        .limit(5)
    
    return (
    channel_wise_df,
    account_type_df,
    customer_avg_df,
    transaction_type_df,
    top5_customer_df
)