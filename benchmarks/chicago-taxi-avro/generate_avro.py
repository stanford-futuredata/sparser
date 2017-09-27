import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

import sys

filename = sys.argv[1]
avro_schema = sys.argv[2]

schema = avro.schema.parse(open(avro_schema).read())  # need to know the schema to write

writer = DataFileWriter(open(filename, "w"), DatumWriter(), schema)
writer.append({"name": "Alyssa"})
writer.append({"name": "Ben", "favorite_number": 256})
writer.append({"name": "Steve", "favorite_number": 1, "favorite_color": "blue"})
writer.close()
