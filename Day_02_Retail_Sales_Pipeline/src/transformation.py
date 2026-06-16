from pyspark.sql.functions import *

def transform_sales_data(valid_df,product_df):
    
    city_df = valid_df.withColumn("store_city",initcap(col("store_city")))

    sale_amount_df = city_df.withColumn("Total_Sales_Amount",col("quantity") * col("unit_price"))

    date_conversion_df = sale_amount_df.withColumn("sale_date",to_date(col("sale_date"),"yyyy-MM-dd"))

    sale_month_df = date_conversion_df.withColumn("sale_month",month(col("sale_date")))

    joined_df = sale_month_df.join(product_df,on="product_id",how="inner")

    return joined_df

