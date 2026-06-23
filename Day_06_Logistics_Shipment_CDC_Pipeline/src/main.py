from pyspark.sql import SparkSession

from src.validation import validate_shipment_data
from src.transformation import (
    find_new_shipments,
    find_updated_shipments,
    find_deleted_shipments,
    transform_shipment_data
)
from src.aggregation import generate_reports


def main():

    spark = SparkSession.builder \
        .appName("LogisticsShipmentCDCPipeline") \
        .getOrCreate()

    # =========================
    # Read Files
    # =========================

    daily_shipments_df = spark.read.csv(
        "data/daily_shipments.csv",
        header=True,
        inferSchema=True
    )

    customer_df = spark.read.csv(
        "data/customer_master.csv",
        header=True,
        inferSchema=True
    )

    existing_df = spark.read.csv(
        "data/existing_shipments.csv",
        header=True,
        inferSchema=True
    )

    # =========================
    # Validation Layer
    # =========================

    valid_df, invalid_df = validate_shipment_data(
        customer_df,
        daily_shipments_df
    )

    print("\n========== VALID RECORDS ==========")
    valid_df.show(truncate=False)

    print("\n========== INVALID RECORDS ==========")
    invalid_df.show(truncate=False)

    # =========================
    # CDC Layer
    # =========================

    new_shipments_df = find_new_shipments(
        valid_df,
        existing_df
    )

    updated_shipments_df = find_updated_shipments(
        valid_df,
        existing_df
    )

    deleted_shipments_df = find_deleted_shipments(
        valid_df,
        existing_df
    )

    print("\n========== NEW SHIPMENTS ==========")
    new_shipments_df.show(truncate=False)

    print("\n========== UPDATED SHIPMENTS ==========")
    updated_shipments_df.show(truncate=False)

    print("\n========== DELETED SHIPMENTS ==========")
    deleted_shipments_df.show(truncate=False)

    # =========================
    # Transformation Layer
    # =========================

    transformed_df = transform_shipment_data(
        valid_df,
        customer_df
    )

    print("\n========== TRANSFORMED DATA ==========")
    transformed_df.show(truncate=False)

    # =========================
    # Aggregation Layer
    # =========================

    (
        region_wise_shipment_cost,
        region_wise_avg_shipment_cost,
        customer_tier_wise_shipment_cost,
        shipment_status_wise_count,
        most_expensive_shipment_by_region
    ) = generate_reports(transformed_df)

    print("\n========== REGION WISE SHIPMENT COST ==========")
    region_wise_shipment_cost.show(truncate=False)

    print("\n========== REGION WISE AVG SHIPMENT COST ==========")
    region_wise_avg_shipment_cost.show(truncate=False)

    print("\n========== CUSTOMER TIER WISE SHIPMENT COST ==========")
    customer_tier_wise_shipment_cost.show(truncate=False)

    print("\n========== SHIPMENT STATUS WISE COUNT ==========")
    shipment_status_wise_count.show(truncate=False)

    print("\n========== MOST EXPENSIVE SHIPMENT BY REGION ==========")
    most_expensive_shipment_by_region.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()