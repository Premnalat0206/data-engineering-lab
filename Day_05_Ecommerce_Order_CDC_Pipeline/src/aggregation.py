from pyspark.sql.functions import *

def generate_reports(transform_df):

    category_wise_revenue = transform_df.groupBy(col("product_category"))\
                                        .agg(sum(col("order_amount")).alias("total_revenue"))
    
    category_wise_avg_order_value = transform_df.groupBy(col("product_category"))\
                                        .agg(avg(col("order_amount")).alias("average_revenue"))
    
    tier_wise_revenue = transform_df.groupBy(col("customer_tier"))\
                                    .agg(sum(col("order_amount")).alias("total_revenue"))
    
    orderStatus_wise_revenue = transform_df.groupBy(col("order_status"))\
                                           .agg(sum(col("order_amount")).alias("total_revenue"))
                                    
    top_5_customerBySpend = transform_df.groupBy(col("customer_id"),col("customer_name"))\
                                        .agg(sum(col("order_amount")).alias("total_spend"))\
                                        .orderBy(col("total_spend").desc())\
                                        .limit(5)
    
    return category_wise_revenue,category_wise_avg_order_value,tier_wise_revenue,orderStatus_wise_revenue,top_5_customerBySpend
