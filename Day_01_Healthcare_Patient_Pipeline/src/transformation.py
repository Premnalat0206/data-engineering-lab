from pyspark.sql.functions import *
def standardize_gender(df):

    transform_gender = df.withColumn("gender",when(col("gender") == "Male","M").otherwise("F"))

    return transform_gender

def create_age_group(df):

    age_group_df = df.withColumn(
        "Age_Group",
        when(col("age") <= 18, "Child")
        .when((col("age") >= 19) & (col("age") <= 35), "Young")
        .when((col("age") >= 36) & (col("age") <= 50), "Adult")
        .otherwise("Senior")
    )

    return age_group_df

def create_fee_category(df):

    fee_category =  df.withColumn("Fee_Category",when(col("consultation_fee")>= 1500 , "Premium")\
                                  .otherwise("Standard"))
    
    return fee_category

def convert_registration_date(df):

    convert_registration_date_df = df.withColumn("registration_date",to_date(col("registration_date")))

    return convert_registration_date_df


def transform_data(df):

    df = standardize_gender(df)

    df = create_age_group(df)

    df = create_fee_category(df)

    df = convert_registration_date(df)

    return df
