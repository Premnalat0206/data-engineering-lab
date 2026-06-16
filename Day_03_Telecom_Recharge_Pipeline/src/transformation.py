from pyspark.sql.functions import *
from pyspark.sql.window import Window

def transform_recharge_data(valid_df):

    # date conversion
    transformed_df = valid_df.withColumn("recharge_date",to_date(col("recharge_date")))

    # recharge_category
    transformed_df = transformed_df.withColumn("recharge_category",
                                               when((col("recharge_amount") > 0 ) & (col("recharge_amount") <= 199),"Basic")
                                               .when((col("recharge_amount") >= 200 ) & (col("recharge_amount") <= 399),"Standard")
                                               .otherwise("Premium")
                                               )

    # month
    transformed_df = transformed_df.withColumn("month",month(col("recharge_date")))

    # latest_recharge_rank

    recharge_rank = Window.partitionBy("customer_id").orderBy(col("recharge_date").desc())
    transformed_df = transformed_df.withColumn("latest_recharge_rank",row_number().over(recharge_rank))

    return transformed_df