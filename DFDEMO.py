from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == '__main__':
    # Create a SparkSession
    spark = SparkSession.builder.master("local[*]").appName("ppp").getOrCreate()

    # Define the schema for the orders dataframe
    orderSchema = StructType([
        StructField("orderid", IntegerType(), nullable=True),
        StructField("orderdate", StringType(), nullable=True),
        StructField("custid", IntegerType(), nullable=True),
        StructField("orderstatus", StringType(), nullable=True)
    ])

    # Read the CSV file with the defined schema
    ordersdf = spark.read.option("header", "true").schema(orderSchema).csv("/user/ec2-user/UKUSMarHDFS/nitin/input.csv")
      # Show the first 5 rows of the dataframe
    ordersdf.show(5)
