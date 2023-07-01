import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class ProcessMessages(beam.DoFn):
    def process(self, element):
        yield element


def run(argv=None):
    # Initialize pipeline options
    parser = argparse.ArgumentParser()
    v, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as p:
        # Sample JSON data as Python lists
        json_data1 = [
            {'id': '4697948','title': 'How do I get two integer values ​from the client by the terminal in this code - GO RPC','creation_date': '2023-06-23T13:21:32'},
            {"id": "3", "name": "Bob"}
        ]

        json_data2 = [
            {"id": "4697948", "age": 25},
            {"id": "2", "age": 30},
        ]

        # Convert Python lists to PCollections
        json_pcoll1 = p | "Create JSON 1 PCollection" >> beam.Create(json_data1)
        json_pcoll2 = p | "Create JSON 2 PCollection" >> beam.Create(json_data2)

        # Map key-value pairs for JSON data
        json_mapped1 = json_pcoll1 | 'Map JSON 1' >> beam.Map(lambda x: (x['id'], x))
        json_mapped2 = json_pcoll2 | 'Map JSON 2' >> beam.Map(lambda x: (x['id'], x))

        # Join the JSON data using CoGroupByKey
        joined_data = ({'json1': json_mapped1, 'json2': json_mapped2}
                       | 'CoGroupByKey' >> beam.CoGroupByKey())

        # Process and print the joined data
        joined_data | 'Print joined data' >> beam.Map(print)


if __name__ == '__main__':
    run()
