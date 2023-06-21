import argparse
import json
from datetime import datetime

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows, WindowInto
from apache_beam.transforms.combiners import Count

class ProcessMessages(beam.DoFn):
    def process(self, element):
        message = element.decode('utf-8')
        print("Received message from PubSub: ", message)
        message_data = json.loads(message)
        message_data["creation_date"] = datetime.strptime(message_data["creation_date"], '%Y-%m-%dT%H:%M:%S.%f')

        yield message_data


class PrintToConsole(beam.DoFn):
    def process(self, element):
        print("Writing to BigQuery: ", element)
        yield element


def run(argv=None):
    parser = argparse.ArgumentParser()
    v,pipleline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipleline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        pubsub_data = (
            pipeline
            | 'Read from PubSub' >> ReadFromPubSub(subscription="projects/streamproc-python-lab/subscriptions/streamProc-StackOverflow-sub")
            | 'Process messages' >> beam.ParDo(ProcessMessages())
            | 'Assign to windows' >> WindowInto(FixedWindows(60))
            | 'Count messages in each window' >> Count.Globally().without_defaults()
            | 'Format for BigQuery' >> beam.Map(lambda count: {'window_count': count})
            | 'Print to console' >> beam.ParDo(PrintToConsole())
            | 'Write to BigQuery' >> WriteToBigQuery(
                table="your_project_id:your_dataset.your_table_name",
                schema='window_count:INTEGER',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        )

if __name__ == '__main__':
    run()
