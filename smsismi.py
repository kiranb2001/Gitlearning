from pyspark.sql import SparkSession
from pyspark.sql.functions import when

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Update IMSI in ORC File") \
    .getOrCreate()

# HDFS input path for the ORC file
input_orc_path = "/user/roamware/output/crm_functional/tripmetrics"

# Desired output path in HDFS
output_orc_path = "/user/roamware/output/crm_functional/tripmetrics_sms"

# The old IMSI value to be replaced
old_imsi_value = 311480565376883L

# The new IMSI value to replace the old one
new_imsi_value = 424030262838565L

# Load the ORC file from HDFS
df = spark.read.format("orc").load(input_orc_path)

# Update the IMSI value
updated_df = df.withColumn(
    "imsi", 
    when(df["imsi"] == old_imsi_value, new_imsi_value).otherwise(df["imsi"])
)

# Save the updated DataFrame back to HDFS as ORC

updated_df.write.mode("append").partitionBy("roamertype","tripstatus","triptype","bintime").format("orc").save(output_orc_path)

# Stop the Spark session
spark.stop()

