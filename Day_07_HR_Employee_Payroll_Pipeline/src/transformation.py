from pyspark.sql.functions import *
from pyspark.sql.window import *

def find_new_employees(valid_df,existing_df):

    new_employees_df = valid_df.join(existing_df,
                                     on="employee_id",
                                     how="left_anti")
    
    new_employees_df.show(truncate=False)
    
    
    return new_employees_df

def find_updated_employees(valid_df,existing_df):
    
    updated_employees_df = valid_df.alias("v").join(existing_df.alias("e"),
                                         on="employee_id",
                                         how = "inner")
    
    updated_employees_df = updated_employees_df.filter(
                                                     (col("v.designation") != col("e.designation"))\
                                                      |\
                                                      (col("v.salary") != col("e.salary"))\
                                                      |\
                                                      (col("v.department_id") != col("e.department_id"))\
                                                      |\
                                                      (col("v.status") != col("e.status"))
    )

    updated_employees_df = updated_employees_df.select("v.*")
    
    return updated_employees_df
    
def find_deleted_employees(valid_df,existing_df):

    deleted_employee_df = existing_df.join(
                                           valid_df,
                                           on="employee_id",
                                           how="left_anti"
                                           )
    
    return deleted_employee_df

def transformed_data(valid_df,department_df):

    transformed_df = valid_df.join(department_df,on="department_id",how="left")

    transformed_df = transformed_df.repartition("department_id")

    transformed_df = transformed_df.withColumn("salary_band",when(col("salary") <= 60000,"LOW")
                                                            .when((col("salary") >= 60001) & (col("salary") <= 90000),"Medium")
                                                            .otherwise("High")
                                               )
    
    transform_partition = Window.partitionBy("department_id").orderBy(desc("salary"))

    transformed_df = transformed_df.withColumn("salary_rank_in_department",dense_rank().over(transform_partition))

    return transformed_df