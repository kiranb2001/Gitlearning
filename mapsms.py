from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import random

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Update ORC Columns with Random Values") \
    .getOrCreate()

# Load ORC file from HDFS
df = spark.read.orc("/user/roamware/output/crm_functional/map_sms_encoding")

# Define function to generate random values for `tpoa`, `tpda`, `smstext`
def get_random_tpda():
    return random.choice(["0522958303", "971509777968", "5566"])

def get_random_smstext():
    return random.choice([" 43617368205769746864726177616C206F662041454420312C3530302E30302077697468204465626974204361726420656E64696E672037313637206174204A554D4549524148204C414B45206B206F7572206F6666657273206469616C202A31333523206F722073696D706C792075736520746865206475206170702E61652F726567697374726174696F6E40","596F75722063757272656E742062616C616E63652069732041454420342E39352E0A5573652074686520647520417070207777772E64752E61652F6D7961707020746F20636865636B20796F757374206469616C202A30353523"])

def get_random_tpoa():
    return random.choice(["ADCBAlert", "FreshToHome", "EI SMS", "237673339965", "971509922589", "971504081786", "3054", "CashNow@", "MultiInfo", "Aramex", "971523869934", "971529023264", "KFC", "CITIBANK", "971558136055", "19094941992", "NAZAHA", "1080", "StanChart", "971552340892", "FBM", "Arab Bank", "4713", "971563592291", "RAKBANK@", "971551131726", "971567871503", "ADIB", "97143699188", "AD-Cherwell", "93764660576", "EDC", "923481348449", "Telegram", "971552775661", "Tinder", "AsterClinic", "97144376475", "971509075075", "256705632730", "wifiuae@", "EXPRESS@", "MOHRE", "etisalatINF", "971582107766", "97126211153", "971525288319", "noon", "RTA", "EmiratesNBD", "5566", "AUTHMSG@", "971569888785", "Kaizala@", "AlhosnApp", "971562599498", "971507699840", "AD-du.", "Dubizzle", "Twitter@", "971567482453", "IBKR", "DIB-SMS@", "971565072646", "du roaming", "EXPOEXPRESS", "ITC", "AD-ANSARIEX", "971522717255", "971507858596", "AD-#Smiles", "NMCROYALSHJ", "971543582705", "2506", "du.", "971501235779", "971529281299", "du", "TheFirstGrp", "FAB", "YAP", "HSBCME", "Spotify@", "AD-MOEDubai", "eToro", "EtihadWE", "AD-SERNITY", "CBD", "SALIK", "97143685027", "CLUBAPPAREL", "Amazon", "SGH SHARJAH", "PURE HEALTH", "4724", "971524118057", "GMDC", "AlHilal@", "971588055934", "Liv"])

# Register the random functions as PySpark UDFs if necessary
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

random_tpoa_udf = udf(get_random_tpoa, StringType())
random_tpda_udf = udf(get_random_tpda, StringType())
random_smstext_udf = udf(get_random_smstext, StringType())

# Apply conditions based on `eventtype` and `errorcode` to update `tpoa`, `tpda`, and `smstext` columns
df_updated = df.withColumn(
    "tpda",
    when((col("eventtype") == 10) & (col("errorcode") == 0), random_tpda_udf())
    .otherwise(col("tpda"))
).withColumn(
    "tpoa",
    when((col("eventtype") == 11) & (col("errorcode") == 1), random_tpoa_udf())
    .otherwise(col("tpoa"))
).withColumn(
    "smstext",
    when((col("eventtype") == 10) | (col("eventtype") == 11), random_smstext_udf())
    .otherwise(col("smstext"))
)

# Write the updated DataFrame back to HDFS in ORC format
df_updated.write.mode("append").partitionBy("roamertype","bintime").format("orc").save("/user/roamware/output/crm_functional/map_sms_kiran")

# Stop the Spark session
spark.stop()

