from pyspark.sql.functions import *
from pyspark.sql.window import Window


def validate_recharge_data(recharge_df, customer_df):

    # Customer Validation (LEFT JOIN)
    validation_df = recharge_df.join(
        customer_df,
        on="customer_id",
        how="left"
    )

    # Duplicate Validation using Composite Key
    duplicate_window = Window.partitionBy(
        "customer_id",
        "recharge_date",
        "recharge_amount"
    ).orderBy(col("recharge_id"))

    validation_df = validation_df.withColumn(
        "duplicate_rank",
        row_number().over(duplicate_window)
    )

    # Error Reason
    validation_df = validation_df.withColumn(
        "error_reason",
        when(
            col("recharge_amount") <= 0,
            "INVALID_AMOUNT"
        )
        .when(
            ~col("channel").isin("APP", "STORE", "WEB"),
            "INVALID_CHANNEL"
        )
        .when(
            col("customer_name").isNull(),
            "INVALID_CUSTOMER"
        )
        .when(
            col("duplicate_rank") > 1,
            "DUPLICATE_RECHARGE"
        )
        .otherwise(None)
    )

    # Valid Records
    valid_df = validation_df.filter(
        col("error_reason").isNull()
    )

    # Invalid Records
    invalid_df = validation_df.filter(
        col("error_reason").isNotNull()
    )

    return valid_df, invalid_df