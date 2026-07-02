from pyspark.sql.functions import *

def aggregate_data(incremental_df):

    # Branch-wise Claim Amount
    branch_wise = incremental_df.groupBy("branch").agg(
        sum("claim_amount").alias("total_claim_amount")
    )

    # Claim-Type-wise Claim Amount
    claim_type_wise = incremental_df.groupBy("claim_type").agg(
        sum("claim_amount").alias("total_claim_amount")
    )

    # Risk-Category-wise Claim Amount
    risk_category_wise = incremental_df.groupBy("risk_category").agg(
        sum("claim_amount").alias("total_claim_amount")
    )

    # Customer-wise Total Claims
    customer_wise = incremental_df.groupBy(
        "customer_id",
        "customer_name"
    ).agg(
        sum("claim_amount").alias("total_claim_amount")
    )

    # Top 10 Customers by Cumulative Claims
    top_10_customers = (
        incremental_df
        .groupBy("customer_id", "customer_name")
        .agg(
            max("cumulative_claim_amount").alias("cumulative_claim_amount")
        )
        .orderBy(col("cumulative_claim_amount").desc())
        .limit(10)
    )

    return (
        branch_wise,
        claim_type_wise,
        risk_category_wise,
        customer_wise,
        top_10_customers
    )