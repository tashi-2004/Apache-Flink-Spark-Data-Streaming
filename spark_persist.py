#Task2
#Persist Data in Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Persist CSV Data in Apache Spark") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

input_path = "/home/tashi/yellow_tripdata_2024-01.csv"
df = spark.read.option("header", "true").csv(input_path)
print("Original Data:")
df.show(10)

processed_df = df.select(
    df["tpep_pickup_datetime"],
    df["passenger_count"].cast("int"),
    df["trip_distance"].cast("float"),
    df["fare_amount"].cast("float"),
    df["tip_amount"].cast("float"),
    df["total_amount"].cast("float")
)

print("Processed Data:")
processed_df.show(5)
output_path = "/home/tashi/spark_persisted_output"
processed_df.write.mode("overwrite").parquet(output_path)

print(f"Data persisted successfully at: {output_path}")

