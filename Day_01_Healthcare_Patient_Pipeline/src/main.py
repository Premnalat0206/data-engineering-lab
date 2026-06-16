from src.validation import (
    add_error_reason,
    split_valid_invalid,
    generate_validation_metrics
)

from src.transformation import transform_data

from src.aggregation import generate_kpis

from pyspark.sql import SparkSession


def main():

    spark = SparkSession.builder \
        .appName("HealthcarePatient") \
        .getOrCreate()

    # Read Data
    df = spark.read.csv(
        "data/patient_registration.csv",
        header=True,
        inferSchema=True
    )

    # Validation Layer
    df = add_error_reason(df)

    valid_df, invalid_df = split_valid_invalid(df)

    metrics = generate_validation_metrics(
        df,
        valid_df,
        invalid_df
    )

    # Write Invalid Records
    # invalid_df.write \
    #     .mode("overwrite") \
    #     .csv("output/invalid_records", header=True)

    # Transformation Layer
    transformed_df = transform_data(valid_df)

    # Write Clean Data
    # transformed_df.write \
    #     .mode("overwrite") \
    #     .csv("output/transformed_data", header=True)

    # Aggregation Layer
    kpi_results = generate_kpis(transformed_df)

    # Write KPI Reports
    # kpi_results["department_count"] \
    #     .write.mode("overwrite") \
    #     .csv("output/kpi_reports/department_count", header=True)

    # kpi_results["department_revenue"] \
    #     .write.mode("overwrite") \
    #     .csv("output/kpi_reports/department_revenue", header=True)

    # kpi_results["city_count"] \
    #     .write.mode("overwrite") \
    #     .csv("output/kpi_reports/city_count", header=True)

    # kpi_results["city_revenue"] \
    #     .write.mode("overwrite") \
    #     .csv("output/kpi_reports/city_revenue", header=True)

    # print(metrics)

    # print("Highest Revenue Department:")
    # print(kpi_results["highest_revenue_department"])

    print("===== VALIDATION METRICS =====")
    print(metrics)

    print("===== INVALID RECORDS =====")
    invalid_df.show(truncate=False)

    print("===== TRANSFORMED DATA =====")
    transformed_df.show(truncate=False)

    print("===== DEPARTMENT PATIENT COUNT =====")
    kpi_results["department_count"].show()

    print("===== DEPARTMENT REVENUE =====")
    kpi_results["department_revenue"].show()

    print("===== CITY PATIENT COUNT =====")
    kpi_results["city_count"].show()

    print("===== CITY REVENUE =====")
    kpi_results["city_revenue"].show()

    print("===== HIGHEST REVENUE DEPARTMENT =====")
    print(kpi_results["highest_revenue_department"])

    spark.stop()


if __name__ == "__main__":
    main()
