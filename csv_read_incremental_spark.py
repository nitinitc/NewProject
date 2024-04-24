from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max, when, to_date

# Create a SparkSession
spark=SparkSession.builder \
    .appName("CSV to PostgreSQL") \
    .getOrCreate()

try:
    # Read the CSV file into a DataFrame

    # Read the CSV file into a DataFrame
    csv_path="/user/ec2-user/UKUSMarHDFS/nitin/member_schemes.csv"  # Adjust the path as needed
    df=spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

    # Replace '\N' values with None or appropriate default value
    df=df.na.fill(None)

    # csv_path, header=True, inferSchema=True)

    # Replace '\N' values with None or appropriate default value

    # Convert string values to appropriate data types if needed
    df=df.withColumn("scheme_alooted_date",
                     when(col("scheme_alooted_date").isin('0', '1'), None)
                     .otherwise(to_date(col("scheme_alooted_date"), "yyyy-MM-dd")))

    # Check the last loaded datetime value from the database
    last_loaded_datetime=spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "member_schemes") \
        .option("user", "consultants") \
        .option("password", "welcomeitc@2022") \
        .load().agg(max("id")).collect()[0][0]

    # Filter DataFrame to include only new rows
    if last_loaded_datetime is not None:
        df=df.filter(col("scheme_alooted_date").isNull() | (col("scheme_alooted_date") > lit(last_loaded_datetime)))

    # Write DataFrame to PostgreSQL database
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb") \
        .option("dbtable", "member_schemes") \
        .option("driver", "org.postgresql.Driver") \
        .option("user", "consultants") \
        .option("password", "welcomeitc@2022") \
        .mode("append") \
        .save()

    print("Data inserted into PostgreSQL table 'member_schemes' successfully!")

except Exception as e:
    print("Error:", e)

finally:
    # Stop SparkSession
    spark.stop()

