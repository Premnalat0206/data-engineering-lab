from pyspark.sql.functions import *

def department_patient_count(df):
    patient_count_df = df.groupBy("department").agg(count("*").alias("department_patient_count"))

    return patient_count_df

def department_revenue(df):
    department_revenue_df = df.groupBy("department").agg(sum(col("consultation_fee")).alias("department_revenue"))
    
    return department_revenue_df



def city_patient_count(df):
    
    city_patient_count_df = df.groupBy("city").agg(count("*").alias("city_patient_count"))
    
    return city_patient_count_df

def city_revenue(df):
    city_revenue_df = df.groupBy("city").agg(sum(col("consultation_fee")).alias("city_revenue_df"))

    return city_revenue_df

def highest_revenue_department(df):
    
    revenue_df = department_revenue(df)

    highest_revenue_department_df = revenue_df.orderBy(col("department_revenue").desc())

    return highest_revenue_department_df.first()


def generate_kpis(df):

    department_count = department_patient_count(df)

    department_rev = department_revenue(df)

    city_count = city_patient_count(df)

    city_rev = city_revenue(df)

    highest_dept = highest_revenue_department(df)

    return {
    "department_count": department_count,
    "department_revenue": department_rev,
    "city_count": city_count,
    "city_revenue": city_rev,
    "highest_revenue_department": highest_dept
}