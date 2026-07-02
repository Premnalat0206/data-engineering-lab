from pyspark.sql.functions import *

def department_employee_count(transformed_df):

    employee_count_df = transformed_df.groupBy("department_id", "department_name") \
                                      .agg(count("*").alias("employee_count"))

    return employee_count_df


def department_payroll_cost(transformed_df):

    payroll_df = transformed_df.groupBy("department_id", "department_name") \
                               .agg(sum("salary").alias("total_payroll"))

    return payroll_df


def salary_band_employee_count(transformed_df):

    salary_band_df = transformed_df.groupBy("salary_band") \
                                   .agg(count("*").alias("employee_count"))

    return salary_band_df


def department_average_salary(transformed_df):

    average_salary_df = transformed_df.groupBy("department_id", "department_name") \
                                      .agg(avg("salary").alias("average_salary"))

    return average_salary_df


def highest_paid_employee(transformed_df):

    highest_paid_df = transformed_df.filter(
        col("salary_rank_in_department") == 1
    )

    return highest_paid_df