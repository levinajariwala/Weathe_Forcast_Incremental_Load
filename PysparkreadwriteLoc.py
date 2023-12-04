# This code is meant for incremental load. The code will match the hive table 
# rows with Postgres table based on a unique key and append any additonal new record created 
# in Postgres to Hive

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("PostgresToHive") \
    .enableHiveSupport() \
    .getOrCreate()

# Hive table
hive_database = "bduk_test1"
hive_table_name = "location1"
hive_table_full_name = "{}.{}".format(hive_database, hive_table_name)

# Find the max(id) in the Hive table
hive_max_id = spark.sql("SELECT MAX(id) as max_id FROM {}".format(hive_table_full_name)).collect()[0]['max_id']

# PostgreSQL database URL
postgres_db_url = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"

# Define PostgreSQL query to get rows with id > hive_max_id
postgres_query = "(SELECT * FROM location WHERE id > {}) AS LOC".format(hive_max_id)

# Read data from PostgreSQL to Spark DataFrame
df_postgres = spark.read.format("jdbc").option("url", postgres_db_url) \
    .option("driver", "org.postgresql.Driver").option("dbtable", postgres_query) \
    .option("user", "consultants").option("password", "WelcomeItc@2022").load()

# Show DataFrame content
df_postgres.show()

# Check if there are extra rows in PostgreSQL
if df_postgres.count() > 0:
    # Append new rows to Hive table
    df_postgres.write.mode("append").format("hive").saveAsTable(hive_table_full_name)
    print("Appended {} new records to Hive table.".format(df_postgres.count()))
else:
    print("No new rows in PostgreSQL table.")

# Stop the Spark session
spark.stop()
