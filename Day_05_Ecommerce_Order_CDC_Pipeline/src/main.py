from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.validation import validate_customer
from src.transformation import (
    find_new_orders,
    find_updated_orders,
    transformed_orders
)
from src.aggregation import generate_reports


def main():

    # Create Spark Session
    spark = SparkSession.builder \
        .appName("EcommerceOrderCDC") \
        .getOrCreate()

    # Read Files
    customer_df = spark.read.csv(
        "data/customer_master.csv",
        header=True,
        inferSchema=True
    )

    dailyorder_df = spark.read.csv(
        "data/daily_orders.csv",
        header=True,
        inferSchema=True
    )

    existingOrder_df = spark.read.csv(
        "data/existing_orders.csv",
        header=True,
        inferSchema=True
    )

    # Validation
    valid_df, invalid_df = validate_customer(
        customer_df,
        dailyorder_df
    )

    print("===== VALID RECORDS =====")
    print("Valid Count :", valid_df.count())
    valid_df.show(truncate=False)

    print("===== INVALID RECORDS =====")
    print("Invalid Count :", invalid_df.count())
    invalid_df.show(truncate=False)

    # CDC - New Orders
    new_record_df = find_new_orders(
        valid_df,
        existingOrder_df
    )

    print("===== NEW ORDERS =====")
    print("New Orders Count :", new_record_df.count())
    new_record_df.show(truncate=False)

    # CDC - Updated Orders
    updated_record_df = find_updated_orders(
        valid_df,
        existingOrder_df
    )

    print("===== UPDATED ORDERS =====")
    print("Updated Orders Count :", updated_record_df.count())
    updated_record_df.show(truncate=False)

    # Transform Data
    transformed_df = transformed_orders(valid_df)

    print("===== TRANSFORMED DATA =====")
    transformed_df.show(truncate=False)

    # Generate Reports
    (
        category_wise_revenue,
        average_order_value,
        tier_wise_revenue,
        orderStatus_wise_revenue,
        top_5_customerBySpend
    ) = generate_reports(transformed_df)

    print("===== CATEGORY WISE REVENUE =====")
    category_wise_revenue.show(truncate=False)

    print("===== CATEGORY WISE AVG ORDER VALUE =====")
    average_order_value.show(truncate=False)

    print("===== CUSTOMER TIER REVENUE =====")
    tier_wise_revenue.show(truncate=False)

    print("===== ORDER STATUS REVENUE =====")
    orderStatus_wise_revenue.show(truncate=False)

    print("===== TOP 5 CUSTOMERS =====")
    top_5_customerBySpend.show(truncate=False)

    # DQ Metrics
    total_records = dailyorder_df.count()

    valid_records = valid_df.count()

    invalid_records = invalid_df.count()

    invalid_customer_count = invalid_df.filter(
        col("error_reason").contains("INVALID_CUSTOMER")
    ).count()

    invalid_quantity_count = invalid_df.filter(
        col("error_reason").contains("INVALID_QUANTITY")
    ).count()

    invalid_amount_count = invalid_df.filter(
        col("error_reason").contains("INVALID_AMOUNT")
    ).count()

    invalid_status_count = invalid_df.filter(
        col("error_reason").contains("INVALID_STATUS")
    ).count()

    print("===== DQ METRICS =====")

    print("Total Records :", total_records)
    print("Valid Records :", valid_records)
    print("Invalid Records :", invalid_records)

    print("Invalid Customer Count :", invalid_customer_count)
    print("Invalid Quantity Count :", invalid_quantity_count)
    print("Invalid Amount Count :", invalid_amount_count)
    print("Invalid Status Count :", invalid_status_count)

    spark.stop()


if __name__ == "__main__":
    main()

