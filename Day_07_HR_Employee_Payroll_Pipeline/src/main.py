from pyspark.sql import SparkSession

from src.validation import validate_hr_data
from src.transformation import (
    find_new_employees,
    find_updated_employees,
    find_deleted_employees,
    transformed_data
)

from src.aggregation import (
    department_employee_count,
    department_payroll_cost,
    salary_band_employee_count,
    department_average_salary,
    highest_paid_employee
)


def main():

    spark = SparkSession.builder \
                        .appName("HR Employee Payroll Pipeline") \
                        .getOrCreate()

    # Read Files
    daily_df = spark.read.csv(
        "data/daily_employee_snapshot.csv",
        header=True,
        inferSchema=True
    )

    department_df = spark.read.csv(
        "data/department_master.csv",
        header=True,
        inferSchema=True
    )

    existing_df = spark.read.csv(
        "data/existing_employee_master.csv",
        header=True,
        inferSchema=True
    )

    # Validation
    valid_df, invalid_df = validate_hr_data(
        daily_df,
        department_df
    )

    print("\n========== VALID RECORDS ==========")
    valid_df.show(truncate=False)

    print("\n========== INVALID RECORDS ==========")
    invalid_df.show(truncate=False)

    # CDC
    new_employee_df = find_new_employees(
        valid_df,
        existing_df
    )

    updated_employee_df = find_updated_employees(
        valid_df,
        existing_df
    )

    deleted_employee_df = find_deleted_employees(
        valid_df,
        existing_df
    )

    print("\n========== NEW EMPLOYEES ==========")
    new_employee_df.show(truncate=False)

    print("\n========== UPDATED EMPLOYEES ==========")
    updated_employee_df.show(truncate=False)

    print("\n========== DELETED EMPLOYEES ==========")
    deleted_employee_df.show(truncate=False)

    # Transformation
    transformed_df = transformed_data(
        valid_df,
        department_df
    )

    print("\n========== TRANSFORMED DATA ==========")
    transformed_df.show(truncate=False)

    # Aggregations
    employee_count_df = department_employee_count(transformed_df)

    payroll_df = department_payroll_cost(transformed_df)

    salary_band_df = salary_band_employee_count(transformed_df)

    average_salary_df = department_average_salary(transformed_df)

    highest_paid_df = highest_paid_employee(transformed_df)

    print("\n========== DEPARTMENT EMPLOYEE COUNT ==========")
    employee_count_df.show(truncate=False)

    print("\n========== DEPARTMENT PAYROLL ==========")
    payroll_df.show(truncate=False)

    print("\n========== SALARY BAND COUNT ==========")
    salary_band_df.show(truncate=False)

    print("\n========== DEPARTMENT AVERAGE SALARY ==========")
    average_salary_df.show(truncate=False)

    print("\n========== HIGHEST PAID EMPLOYEE ==========")
    highest_paid_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()