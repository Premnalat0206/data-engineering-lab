from pyspark.sql.functions import *

def aggregate_sales_data(df):

    department_revenue = df.groupBy("department").agg(sum(col("Total_Sales_Amount")).alias("department_wise_revenue"))

    department_average_sales = df.groupBy("department").agg(avg(col("Total_Sales_Amount")).alias("Average_Sales_Per_Department"))

    city_wise_revenue = df.groupBy("store_city").agg(sum(col("Total_Sales_Amount")).alias("City_Wise_Revenue"))

    product_wise_revenue = df.groupBy("product_name").agg(sum(col("Total_Sales_Amount")).alias("Product_Wise_Revenue"))

    top_revenue_department = department_revenue.orderBy(desc("department_wise_revenue")).limit(1)

    return department_revenue,department_average_sales,city_wise_revenue,product_wise_revenue,top_revenue_department