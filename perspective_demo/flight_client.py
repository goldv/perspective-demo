import pyarrow as pa
import pyarrow.flight
import pyarrow.parquet as pq

client = pa.flight.connect("grpc://localhost:8815")

# data_table = pq.read_table('flights.parquet')
upload_descriptor = pa.flight.FlightDescriptor.for_path('flights.parquet')
# writer, _ = client.do_put(upload_descriptor, data_table.schema)
# writer.write_table(data_table)
# writer.close()

flight = client.get_flight_info(upload_descriptor)
descriptor = flight.descriptor
print("Path:", descriptor.path[0].decode('utf-8'), "Rows:", flight.total_records, "Size:", flight.total_bytes)
print("=== Schema ===")
print(flight.schema)
print("==============")

reader = client.do_get(flight.endpoints[0].ticket)
read_table = reader.read_all()
print(read_table.to_pandas().head())