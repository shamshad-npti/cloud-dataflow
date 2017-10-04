# Getting started with Google Dataflow (Apache Beam)
Apache Beam Quick Start Guide

Dataflow
```python
import apache_beam as beam

# lets have a sample string
data = ["this is sample data", "this is yet another sample data"]

# create a pipeline
pipeline = beam.Pipeline()
counts = (pipeline | "create" >> beam.Create(data)
    | "split" >> beam.ParDo(lambda row: row.split(" "))
    | "pair" >> beam.Map(lambda w: (w, 1))
    | "group" >> beam.CombinePerKey(sum))

# lets collect our result with a map transformation into output array
output = []
def collect(row):
    output.append(row)
    return True

counts | "print" >> beam.Map(collect)

# Run the pipeline
result = pipeline.run()

# lets wait until result a available
result.wait_until_finish()

# print the output
print output
```
