#Task 1
# Stream the data into Flink 
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.schema import Schema
from pyflink.table.types import DataTypes
from pyflink.table import TableDescriptor, FormatDescriptor
from pyflink.table.udf import udf
import math

env = StreamExecutionEnvironment.get_execution_environment()
settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
table_env = StreamTableEnvironment.create(env, environment_settings=settings)
csv_file_path = "/home/tashi/yellow_tripdata_2024-01.csv"

schema = Schema.new_builder() \
    .column("VendorID", DataTypes.STRING()) \
    .column("tpep_pickup_datetime", DataTypes.STRING()) \
    .column("tpep_dropoff_datetime", DataTypes.STRING()) \
    .column("passenger_count", DataTypes.STRING()) \
    .column("trip_distance", DataTypes.STRING()) \
    .column("RatecodeID", DataTypes.STRING()) \
    .column("store_and_fwd_flag", DataTypes.STRING()) \
    .column("PULocationID", DataTypes.STRING()) \
    .column("DOLocationID", DataTypes.STRING()) \
    .column("payment_type", DataTypes.STRING()) \
    .column("fare_amount", DataTypes.STRING()) \
    .column("extra", DataTypes.STRING()) \
    .column("mta_tax", DataTypes.STRING()) \
    .column("tip_amount", DataTypes.STRING()) \
    .column("tolls_amount", DataTypes.STRING()) \
    .column("improvement_surcharge", DataTypes.STRING()) \
    .column("total_amount", DataTypes.STRING()) \
    .column("congestion_surcharge", DataTypes.STRING()) \
    .column("Airport_fee", DataTypes.STRING()) \
    .build()

table_env.create_temporary_table(
    "SourceTable",
    TableDescriptor.for_connector("filesystem")
    .schema(schema)
    .option("path", csv_file_path)
    .format(
        FormatDescriptor.for_format("csv")
        .option("field-delimiter", ",")
        .option("quote-character", "\"")
        .option("ignore-first-line", "true") 
        .build()
    )
    .build(),
)

@udf(result_type=DataTypes.FLOAT())
def parse_float(value):
    try:
        return float(value)
    except ValueError:
        return math.nan  

@udf(result_type=DataTypes.INT())
def parse_int(value):
    try:
        return int(value)
    except ValueError:
        return None  

table_env.create_temporary_function("parse_float", parse_float)
table_env.create_temporary_function("parse_int", parse_int)


result_table = table_env.sql_query("""
    SELECT 
        tpep_pickup_datetime, 
        parse_int(passenger_count) AS passenger_count,
        parse_float(trip_distance) AS trip_distance,
        parse_float(fare_amount) AS fare_amount,
        parse_float(tip_amount) AS tip_amount,
        parse_float(total_amount) AS total_amount
    FROM SourceTable
    WHERE trip_distance IS NOT NULL AND fare_amount IS NOT NULL
""")

table_env.to_changelog_stream(result_table).print()
env.execute("Flink CSV Streaming Job")

