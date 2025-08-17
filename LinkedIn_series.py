# Databricks notebook source
# MAGIC %md 1. Split the vales of column into multiple rows( input dataset contains a column with multiple comma-seperated values)

# COMMAND ----------

#Given data by interviewer
data = [(1,"apple,banana,orange"),
        (2,"mango,grapes"),
        (3,"pineapple")]
columns = ["ID","Tags"]

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode,split

spark = SparkSession.builder.appName("ColumnSplit").getOrCreate()
# DataFrame creation
data = [(1,"apple,banana,orange"),(2,"mango,grapes"),(3,"pineapple")]
columns = ["ID","Tags"]
df = spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

# Split Tags columns and use explode to make multiple rows(in last drop Tags from final_df)
final_df = df.withColumn("Tag", explode(split(df["Tags"], ","))).drop("Tags")
final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Perform SCD type 1 to given data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("SCD_Type1").getOrCreate()

# existing customer data (Existing Table)
existing_data = [
    (1, "John Doe", "123 Elm St", "2023-01-01"),
    (2, "Jane Smith", "456 Oak St", "2023-02-01"),
    (3, "Jim Brown", "789 Pine St", "2023-03-01")
]
# New data with updates
new_data = [
    (1, "John Doe", "123 Elm St", "2023-04-01"),  # No change
    (2, "Jane Doe", "456 Oak St", "2023-04-01"),  # Name changed
    (4, "Lucy Green", "101 Maple St", "2023-04-01")  # New customer
]

# Create DataFrames
existing_df = spark.createDataFrame(existing_data, ["customer_id", "name", "address", "last_updated"])
new_df = spark.createDataFrame(new_data, ["customer_id", "name", "address", "last_updated"])


# COMMAND ----------

# MAGIC %md
# MAGIC 3. Perform SCD type 2 to given data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType

# Initialize Spark session
spark = SparkSession.builder.appName("SCD_Type2").getOrCreate()

# Schema for existing data
existing_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("start_date", StringType(), True),  # Or DateType() if you parse date
    StructField("end_date", StringType(), True),    # Or DateType()
    StructField("current_flag", BooleanType(), True)
])

# Existing customer data
existing_data = [
    (1, "Kolu", "123 Elm St", "2023-01-01", None, True),
    (2, "mani", "456 Oak St", "2023-02-01", None, True),
    (3, "ishan", "789 Pine St", "2023-03-01", None, True)
]

# Create DataFrame
existing_df = spark.createDataFrame(existing_data, schema=existing_schema)

# Schema for new data
new_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("start_date", StringType(), True)  # Or DateType()
])

# New incoming data
new_data = [
    (1, "Kolu", "123 Elm St", "2023-04-01"), #No change
    (2, "rahul", "456 Oak St", "2023-04-01"), #Name change
    (4, "roy", "101 Maple St", "2023-04-01")# new data
]

# Create DataFrame
new_df = spark.createDataFrame(new_data, schema=new_schema)

# Show both for verification
existing_df.show()
new_df.show()


# COMMAND ----------

# Alias DataFrames
current_df = existing_df.filter(col("current_flag") == True).alias("old")
new_df = new_df.alias("new")

# Join and resolve ambiguity by qualifying column references
joined_df = current_df.join(new_df, on="customer_id", how="outer") \
    .select(
        col("customer_id"),
        col("old.name").alias("old_name"),
        col("old.address").alias("old_address"),
        col("old.start_date").alias("old_start_date"),
        col("old.end_date"),
        col("old.current_flag"),
        col("new.name").alias("new_name"),
        col("new.address").alias("new_address"),
        col("new.start_date").alias("new_start_date")
    )


# COMMAND ----------

joined_df.display()

# COMMAND ----------

# Detect new or changed records
changed_df = joined_df.filter(
    (col("old_name").isNull()) |  # New customer
    (col("new_name").isNotNull() & (
        (col("old_name") != col("new_name")) |
        (col("old_address") != col("new_address"))
    ))
)

# COMMAND ----------

changed_df.show()

# COMMAND ----------

# Expire old records
expired_df = changed_df.filter(col("old_name").isNotNull()) \
    .select(
        col("customer_id"),
        col("old_name").alias("name"),
        col("old_address").alias("address"),
        col("old_start_date").alias("start_date"),
        current_date().alias("end_date"),
        lit(False).alias("current_flag")
    )

# COMMAND ----------

expired_df.display()

# COMMAND ----------

# Insert new or updated records
new_records_df = changed_df.select(
    col("customer_id"),
    col("new_name").alias("name"),
    col("new_address").alias("address"),
    current_date().alias("start_date"),
    lit(None).cast("date").alias("end_date"),
    lit(True).alias("current_flag")
)

# COMMAND ----------

new_records_df.display()

# COMMAND ----------

# Get unchanged records
unchanged_df = current_df.join(changed_df.select("customer_id"), "customer_id", "left_anti")

# Final SCD Type 2 DataFrame
final_df = unchanged_df.unionByName(expired_df).unionByName(new_records_df)

# Show result
final_df.show()
