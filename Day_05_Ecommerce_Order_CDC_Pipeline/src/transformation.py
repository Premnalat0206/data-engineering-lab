from pyspark.sql.functions import *
from pyspark.sql.window import *

def find_new_orders(valid_df,exisitng_order_df):

    new_orders_df = valid_df.join(exisitng_order_df,
                                  on="order_id",
                                  how="left_anti"
                                  )
    
    return new_orders_df

def find_updated_orders(valid_df,exisiting_order_df):

    joined_df = valid_df.alias("d").join(exisiting_order_df.alias("e"),
                              on="order_id",
                              how="inner")
    
    filter_updated_orders_df = joined_df.filter(
                                               (col("d.order_amount") != col("e.order_amount"))
                                               |
                                               (col("d.order_status") != col("e.order_status")) 
                                             )
    filter_updated_orders_df = filter_updated_orders_df.select("d.*")

    return filter_updated_orders_df

def transformed_orders(valid_df):

    transform_df = valid_df.withColumn("order_size",
                                        when(col("order_amount") <= 5000,"Small")
                                        .when(col("order_amount") <= 20000,"Medium")
                                        .otherwise("Large")
                                    )
    
    window_spec = Window.partitionBy("product_category").orderBy(col("order_amount").desc())

    transform_df = transform_df.withColumn("category_sales_rank",rank().over(window_spec))

    return transform_df
