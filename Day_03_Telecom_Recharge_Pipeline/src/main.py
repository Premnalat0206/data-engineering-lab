from pyspark.sql import SparkSession
from src.validation import validate_recharge_data
from src.transformation import transform_recharge_data
from src.aggregation import generate_reports

def main():

    spark = SparkSession.builder\
                        .appName("TelecomRecharge")\
                        .getOrCreate()
    
    customer_df = spark.read.csv("data/customer_master.csv",
                                 header= True,
                                 inferSchema=True)
    
    recharge_df = spark.read.csv("data/recharge_transactions.csv",
                                 header=True,
                                 inferSchema=True)
    
    valid_df ,invalid_df = validate_recharge_data(recharge_df,customer_df)

    print("=== Valid Record ===")
    valid_df.show(truncate=False)

    print("=== Invalid Record ===")
    invalid_df.show(truncate=False)

    transformed_df = transform_recharge_data(valid_df)
    print("=== Transformed Data ===")
    transformed_df.show(truncate=False)
    
    print("=== Analytical Reports ===")

    channel_revenue_df,\
    circle_revenue_df,\
    category_revenue_df,\
    customer_revenue_df,\
    top_5_customer_df,\
    = generate_reports(transformed_df)

    print("=== channel_revenue ===")
    channel_revenue_df.show(truncate=False)

    print("=== circle_revenue ===")
    circle_revenue_df.show(truncate=False)

    print("=== category_revenue ===")
    category_revenue_df.show(truncate=False)

    print("=== customer_revenue ===")
    customer_revenue_df.show(truncate=False)

    print("=== top_5_customer ===")
    top_5_customer_df.show(truncate=False)

if __name__ == "__main__":
    main()