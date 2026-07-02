from pyspark.sql import SparkSession
from src.validation import validate_data
from src.transformation import transform_data
from src.aggregation import aggregate_data

def main():
    spark = (
        SparkSession.builder
        .appName("Insurance Claims Fraud Analytics Pipeline")
        .getOrCreate()
    )
    
    
    daily_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/daily_claims.csv")
    )
    
    customer_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/customer_master.csv")
    )
    
    policy_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/policy_master.csv")
    )
    
    processed_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/processed_claims.csv")
    )
    
    valid_df, invalid_df = validate_data(
        daily_df,
        customer_df,
        policy_df
    )
    
    print("\n================ VALID RECORDS ================\n")
    valid_df.show(truncate=False)
    
    print("\n================ INVALID RECORDS ================\n")
    invalid_df.show(truncate=False)
    
    
    transformed_df = transform_data(
        valid_df,
        processed_df,
        customer_df,
        policy_df
    )
    
    print("\n================ TRANSFORMED DATA ================\n")
    transformed_df.show(truncate=False)
    
    
    
    (
        branch_wise,
        claim_type_wise,
        risk_category_wise,
        customer_wise,
        top_10_customers
    ) = aggregate_data(transformed_df)
    
    print("\n========== Branch Wise Claim Amount ==========\n")
    branch_wise.show()
    
    print("\n========== Claim Type Wise Claim Amount ==========\n")
    claim_type_wise.show()
    
    print("\n========== Risk Category Wise Claim Amount ==========\n")
    risk_category_wise.show()
    
    print("\n========== Customer Wise Total Claims ==========\n")
    customer_wise.show()
    
    print("\n========== Top 10 Customers ==========\n")
    top_10_customers.show()
    
    spark.stop()

if __name__ == "__main__":
    main()