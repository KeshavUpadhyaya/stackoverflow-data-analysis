import os
import apache_beam as beam
import json
from datetime import datetime
import matplotlib.pyplot as plt
import collections

# Path to the JSON file
json_file_path = "C:/Users/Thomas/Downloads/TestJSON.json"

output_file_path = f"C:/Users/Thomas/Downloads/OutputJSON_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

class ProcessJSON(beam.DoFn):
    def process(self, element):
        # Extract the creation_date from the JSON element
        creation_date = element["creation_date"]

        # Emit the creation_date as the key and the entire JSON element as the value
        yield creation_date, element

data = []

# Create a pipeline
with beam.Pipeline() as pipeline:
    # Read the JSON file
    json_data = (
            pipeline
            | "Read JSON File" >> beam.io.ReadFromText(json_file_path)
            | "Parse JSON" >> beam.Map(json.loads)
    )

    # Group the JSON entries by creation_date
    grouped_data = (
            json_data
            | "Group by Creation Date" >> beam.ParDo(ProcessJSON())
            | "GroupByKey" >> beam.GroupByKey()
            | "Store results" >> beam.Map(lambda x: data.append((x[0], len(x[1]))))
    )

# The data list now contains the grouped data.
# We'll plot a histogram of the number of elements per group (i.e., per creation_date).
dates, counts = zip(*data)

fig, ax = plt.subplots()
ax.bar(dates, counts)
plt.xticks(fontsize=4)  # Rotate x-axis labels for readability
plt.show()

