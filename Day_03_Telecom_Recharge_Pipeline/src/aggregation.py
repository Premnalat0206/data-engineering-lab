from pyspark.sql.functions import *

def generate_reports(transformed_df):

    channel_revenue_df = transformed_df.groupBy(col("channel")).agg(sum(col("recharge_amount")).alias("channel_revenue"))
    circle_revenue_df = transformed_df.groupBy(col("circle")).agg(sum(col("recharge_amount")).alias("circle_revenue"))
    category_revenue_df = transformed_df.groupBy(col("recharge_category")).agg(sum(col("recharge_amount")).alias("category_revenue"))
    customer_revenue_df = transformed_df.groupBy(col("customer_id")).agg(sum(col("recharge_amount")).alias("customer_revenue"))
    top_5_customers_df = customer_revenue_df.orderBy(col("customer_revenue").desc()).limit(5)

    return circle_revenue_df,channel_revenue_df,category_revenue_df,top_5_customers_df,customer_revenue_df