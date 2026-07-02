from pyspark.sql.functions import *

def validate_hr_data(daily_df,department_df):

    validate_df = daily_df.join(department_df,
                                on="department_id",
                                how="left")
    
    validate_df = validate_df.withColumn("department_error",
                                         when(col("department_name").isNull(),"INVALID_DEPARTMENT")\
                                         .otherwise(None)
                                        )
    
    validate_df = validate_df.withColumn("salary_error",
                                         when(col("salary") <= 0 , "INVALID_SALARY")\
                                         .otherwise(None)
                                        )
    
    validate_df = validate_df.withColumn("status_error",
                                         when(~col("status").isin(["ACTIVE","INACTIVE"]),"INVALID_STATUS")\
                                        .otherwise(None)
                                        )
    
    validate_df = validate_df.withColumn("error_reasons",
                                         concat_ws(",",
                                                   "department_error",
                                                   "salary_error",
                                                   "status_error")
                                        )
    validate_df = validate_df.drop("department_error","salary_error","status_error")

    
    valid_df = validate_df.filter(col("error_reasons") == "")

    invalid_df = validate_df.filter(col("error_reasons") != "")

    total_records = daily_df.count()
    valid_records = valid_df.count()
    invalid_records = invalid_df.count()

    print(f"Total Records: {total_records}")
    print(f"Valid Records: {valid_records}")
    print(f"Invalid Records: {invalid_records}")

    return valid_df,invalid_df
    
