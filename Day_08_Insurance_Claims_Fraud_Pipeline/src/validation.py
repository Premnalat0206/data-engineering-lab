from pyspark.sql.functions import *

def validate_data(daily_df,customer_df,policy_df):

    validated_df = daily_df.join(customer_df,on="customer_id",how="left")

    validated_df = validated_df.join(policy_df,on="policy_id",how="left")

    validated_df = validated_df.withColumn("customer_error"   
                                             ,when(col("customer_name").isNull(),"INVALID_CUSTOMER")  
                                             .otherwise(None)
                                            )
    
    validated_df = validated_df.withColumn("policy_error",
                                             when(col("policy_type").isNull(),"INVALID_POLICY")
                                             .otherwise(None)
                                             )
    
    validated_df = validated_df.withColumn("amount_error", 
                                            when(col("claim_amount") <= 0,"INVALID_AMOUNT")
                                            .otherwise(None)
                                            )
    
    validated_df = validated_df.withColumn("claim_error", 
                                             when(~col("claim_type").isin(["MEDICAL","VEHICLE","HOME"]),"INVALID_CLAIM_TYPE")
                                            .otherwise(None)
                                             )
    
    validated_df = validated_df.withColumn("error_reason",concat_ws(",",
                                                                      "customer_error",
                                                                      "policy_error",
                                                                      "amount_error",
                                                                      "claim_error")
                                         )
    
    validated_df = validated_df.drop("customer_error","policy_error","amount_error","claim_error")
    
    valid_df = validated_df.filter(col("error_reason") == "")

    invalid_df = validated_df.filter(col("error_reason") != "")

    total_records = daily_df.count()
    valid_records = valid_df.count()
    invalid_records = invalid_df.count()

    invalid_customer = validated_df.filter(col("customer_error").isNotNull())
    invalid_customer = invalid_customer.count()

    invalid_policies = validated_df.filter(col("policy_error").isNotNull())
    invalid_policies = invalid_policies.count()

    invalid_claim_type = validated_df.filter(col("claim_error").isNotNull())
    invalid_claim_type = invalid_claim_type.count()

    invalid_claim_amount = validated_df.filter(col("amount_error").isNotNull())
    invalid_claim_amount = invalid_claim_amount.count()

    print(f"Total records : {total_records}")
    print(f"Valid records : {valid_records}")
    print(f"Invalid records : {invalid_records}")
    print(f"Invalid customer : {invalid_customer}")
    print(f"Invalid policies : {invalid_policies}")
    print(f"Invalid claim type : {invalid_claim_type}")
    print(f"Invalid amount : {invalid_claim_amount}")

    return valid_df,invalid_df