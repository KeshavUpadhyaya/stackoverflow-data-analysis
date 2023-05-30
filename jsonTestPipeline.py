import os

import apache_beam as beam
import json
from datetime import datetime

# Path to the JSON file
json_file_path = "C:/Users/Thomas/Studium/SteamProc/TestJSON.json"

output_file_path = f"C:/Users/Thomas/Studium/SteamProc/OutputJSON_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"


class ProcessJSON(beam.DoFn):
    def process(self, element):
        # Extract the creation_date from the JSON element
        creation_date = element["creation_date"]

        # Emit the creation_date as the key and the entire JSON element as the value
        yield creation_date, element


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
    )

    output = (
            grouped_data
            | "Format for Output" >> beam.Map(lambda element: json.dumps({element[0]: element[1]}))
            | "Write to File" >> beam.io.WriteToText(output_file_path)
    )

# Print the grouped data
grouped_data | beam.Map(print)

# Run the pipeline
pipeline.run()
