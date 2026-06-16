from pyspark.sql import SparkSession
from src.validation import validate_sales_data
from src.transformation import transform_sales_data
from src.aggregation import aggregate_sales_data

def main():

    spark = SparkSession.builder\
                        .appName("RetailSalesPipeline")\
                        .getOrCreate()
    
    sales_df = spark.read.csv("data/sales_transactions.csv"\
                    ,header =  True,\
                    inferSchema = True)
    
    product_df = spark.read.csv("data/product_master.csv"
                         ,header=True
                         ,inferSchema=True)
    
    valid_df,invalid_df = validate_sales_data(sales_df,product_df)

    print("INVALID RECORDS")
    invalid_df.show(truncate=False)

    print("VALID RECORDS")
    valid_df.show(truncate=False)

    transformed_df = transform_sales_data(valid_df,product_df)

    print("Transformed Data")
    transformed_df.show(truncate=False)

    department_revenue, \
    department_average_sales, \
    city_wise_revenue, \
    product_wise_revenue, \
    top_revenue_department = aggregate_sales_data(
        transformed_df
    )

    print("DEPARTMENT REVENUE")
    department_revenue.show()

    print("DEPARTMENT AVERAGE SALES")
    department_average_sales.show()

    print("CITY REVENUE")
    city_wise_revenue.show()

    print("PRODUCT REVENUE")
    product_wise_revenue.show()

    print("TOP REVENUE DEPARTMENT")
    top_revenue_department.show()

    print(f"Valid Records: {valid_df.count()}")
    print(f"Invalid Records: {invalid_df.count()}")

if __name__ == "__main__":
    main()





    