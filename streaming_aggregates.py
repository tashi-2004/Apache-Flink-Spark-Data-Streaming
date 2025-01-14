#Task 3
#Calculates the daily sum of passengers and total daily revenue. 
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.common.serialization import Encoder
from pyflink.datastream.connectors import FileSink
import os

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
csv_file_path = "/home/tashi/yellow_tripdata_2024-01.csv"
output_path = "/home/tashi/aggregated_output"
if not os.path.exists(output_path):
    os.makedirs(output_path)

def parse_csv(line):
    fields = line.split(',')
    try:
        pickup_datetime = fields[1].strip().strip('"')
        passenger_count = int(fields[3]) if fields[3].isdigit() else 0
        total_amount = float(fields[16]) if fields[16] else 0.0
        date = pickup_datetime.split(' ')[0]  
        return date, passenger_count, total_amount
    except (IndexError, ValueError):
        return None

lines = []
with open(csv_file_path, "r") as file:
    header = file.readline() 
    for i, line in enumerate(file):
        if i >= 100000:  #You can set according to your memory. Also see the flink-conf.yaml. Update JobManager and TaskManager Size.
            break
        lines.append(line.strip())

data_stream = env.from_collection(lines, type_info=Types.STRING())
parsed_stream = data_stream.map(parse_csv, output_type=Types.TUPLE([Types.STRING(), Types.INT(), Types.FLOAT()]))
aggregated_stream = parsed_stream \
    .filter(lambda x: x is not None) \
    .key_by(lambda x: x[0]) \
    .reduce(lambda a, b: (
        a[0],  # date
        a[1] + b[1],  # sum of passenger counts
        a[2] + b[2]   # sum of total revenue
    ))

file_sink = FileSink.for_row_format(
    output_path,
    Encoder.simple_string_encoder()
).build()

aggregated_stream.map(lambda x: f"{x[0]},{x[1]},{x[2]}") \
    .sink_to(file_sink)

env.execute("Daily Aggregation for First 10,000 Rows")
print("Flink job completed successfully. Results written to:", output_path)

