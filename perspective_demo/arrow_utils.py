import pyarrow as pa
import pyarrow.parquet as pq

table = pq.read_table('flights.parquet')

for f in table.schema:
    print(f'"{f.name}" : {f.type}')


next_table = pa.Table.from_arrays(arrays=pa.compute.add(table.column('DEP_DELAY'), 2), names=['DEP_DELAY'])

stream = pa.BufferOutputStream()
