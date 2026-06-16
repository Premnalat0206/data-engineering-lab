from pyspark.sql.functions import *

def validate_null_product(df):

    invalid_df = df.filter(col("product_id").isNull() | (trim(col("product_id")) == "" ))

    invalid_reasons_df = invalid_df.withColumn("Error_Reason",lit("NULL_PRODUCT"))
    
    return invalid_reasons_df


def validate_duplicate_sale(df):

    duplicate_sale = df.groupby("sale_id").agg(count(col("sale_id")).alias("count"))

    invalid_sale = duplicate_sale.filter(col("count") > 1)

    join_df =  df.join(invalid_sale,on="sale_id",how= "inner")

    final_df = join_df.withColumn("Error_Reason",lit("DUPLICATE_SALE"))

    return final_df.drop(col("count"))
    

def validate_invalid_quantity(df):

    invalid_quantity = df.filter(col("quantity") <= 0 )

    final_df = invalid_quantity.withColumn("Error_Reason",lit("INVALID_QUANTITY"))
    
    return final_df
    

def validate_product_master(df, product_df):

    invalid_id = df.join(product_df,on="product_id",how="left_anti")

    final_df = invalid_id.withColumn("Error_Reason",lit("INVALID_PRODUCT"))
    
    return final_df

def validate_sales_data(df, product_df):

    null_df = validate_null_product(df)

    duplicate_df = validate_duplicate_sale(df)

    quantity_df = validate_invalid_quantity(df)

    product_invalid_df = validate_product_master(df, product_df)

    invalid_df = null_df.union(duplicate_df).union(quantity_df).union(product_invalid_df)

    invalid_df = invalid_df.dropDuplicates(["sale_id"])

    valid_df = df.join(invalid_df,on="sale_id",how="left_anti")

    return valid_df,invalid_df





