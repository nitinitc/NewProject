from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, max, when, to_date

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CSV to Hive External Table") \
    .enableHiveSupport() \
    .getOrCreate()

try:
    # Read the CSV file into a DataFrame
    csv_path = "UKUSMarHDFS/nitin/member_schemes.csv"

    # Define the schema for the DataFrame
    member_sch = """
        id INT,
        user_id INT,
        family_id STRING,
        member_id STRING,
        scheme_id STRING,
        sent_approve INT,
        scheme_alooted_date TIMESTAMP,
        zone STRING,
        district_id STRING,
        block_code STRING,
        ward_village_code STRING,
        othersubscheme STRING,
        scheme_comment STRING,
        otherdepartment STRING,
        otherscheme STRING,
        scheme_department_id STRING,
        sch_cat_dep_rel_id STRING,
        newdepart_id INT,
        newschm_id INT,
        reject_status INT,
        approved_by_district_user_id STRING,
        rejected_by_district_user_id STRING,
        sention_remarks STRING,
        sention_status INT,
        sanction_date TIMESTAMP,
        noscheme INT,
        transfer_from_dep STRING,
        transfer_from_scheme STRING,
        transfer_from_dist STRING,
        loan_amount STRING,
        incomeIncreaseUpto STRING,
        radiobuttonStatus STRING,
        member_scheme_active BOOLEAN
    """

    df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

    # Convert string values to appropriate data types if needed
    df = df.withColumn("scheme_alooted_date",
                       when(col("scheme_alooted_date").isin('0', '1') | col("scheme_alooted_date").isNull(), lit(None))
                       .otherwise(to_date(col("scheme_alooted_date"), "yyyy-MM-dd")))

    # Define the Hive database and table name
    hive_database = "itc_project"
    hive_table_name = "member_schemes"
    hive_table_location = f"/user/hive/warehouse/{hive_database}.db/{hive_table_name}"

    # Create external Hive table
    df.write.format("parquet").mode("overwrite") \
        .option("path", hive_table_location) \
        .saveAsTable(f"{hive_database}.{hive_table_name}")

    print(f"External table '{hive_table_name}' created successfully in database '{hive_database}' at location: {hive_table_location}")

except Exception as e:
    print("Error:", e)

finally:
    # Stop SparkSession
    spark.stop()
