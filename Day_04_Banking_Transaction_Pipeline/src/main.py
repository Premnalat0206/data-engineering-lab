from pyspark.sql import SparkSession
from src.validation import validate_transactions
from src.transformation import transformed_data
from src.aggregation import generate_reports

def main():

    spark = SparkSession.builder\
                        .appName("BankingTransaction")\
                        .getOrCreate()
    
    customer_df = spark.read.csv(
                                 "data/customer_master.csv",
                                 header=True,
                                 inferSchema=True
    )

    daily_df = spark.read.csv(
                              "data/daily_transactions.csv",
                              header=True,
                              inferSchema=True
    )

    processed_df = spark.read.csv(
                                "data/processed_transactions.csv",
                                header=True,
                                inferSchema=True
    )

    valid_df ,invalid_df = validate_transactions(daily_df,customer_df)

    print("=== Invalid Data ===")
    invalid_df.show(truncate=False)

    transformed_df = transformed_data(valid_df,processed_df,customer_df)
    print("=== Transformed Data === ")
    transformed_df.show()

    channel_wise_df,\
    account_type_df,\
    customer_avg_df,\
    transaction_type_df,\
    top5_customer_df = generate_reports(transformed_df)
    
    print("=== Channel Wise Revenue ===")
    channel_wise_df.show()

    print("=== Account type Wise Revenue ===")
    account_type_df.show()

    print("=== Average Customer Revenue ===")
    customer_avg_df.show()

    print("=== trnsaction Type Revenue ===")
    transaction_type_df.show()

    print("=== Top 5 Customer ===")
    top5_customer_df.show()

    spark.stop()

if __name__ == "__main__":
    main()