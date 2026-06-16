from pyspark.sql.functions import *

def find_null_names(df):
    null_names = df.filter(col("patient_name").isNull())
    return null_names

def find_duplicate_patients(df):
    
    duplicate_records = df.groupby(col("patient_id")).agg(count(col("patient_id")).alias("count"))
    
    filtered_duplicate = duplicate_records.filter(col("count") > 1)

    finaldf = df.join(filtered_duplicate,on="patient_id",how = "inner")

    return finaldf

def find_invalid_age(df):
    invalid_age = df.filter(col("age") <= 0)

    return invalid_age

def add_error_reason(df):

    df = df.withColumn(
        "error_reason",
        when(col("patient_name").isNull(), "NULL_NAME")
        .when(col("age") <= 0, "INVALID_AGE")
        .otherwise(None)
    )

    return df

def split_valid_invalid(df):

    valid_df = df.filter(col("error_reason").isNull())

    invalid_df = df.filter(col("error_reason").isNotNull())

    return valid_df,invalid_df

def generate_validation_metrics(df,valid_df,invalid_df):

    total_records = df.count()

    valid_records = valid_df.count()
    
    invalid_records = invalid_df.count()

    return {
    "total_records": total_records,
    "valid_records": valid_records,
    "invalid_records": invalid_records
}