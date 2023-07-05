import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterCount, AccumulationMode, AfterProcessingTime
from datasketch import HyperLogLog
from apache_beam.io.gcp.bigquery import WriteToBigQuery

ACCURACY = 14
PROJECT_ID = 'stream-processing-384807'
DATASET_ID = 'stackoverflow'
TABLE_ID = 'unique-tag-count'


class AddWindowInfo(beam.DoFn):
    def process(self, x, window=beam.DoFn.WindowParam):
        d = {}
        d['count'] = float(x)
        d["window_start"] = window.start.to_utc_datetime()
        d["window_end"] = window.end.to_utc_datetime()
        print(d)
        yield d


# Custom combine function for HyperLogLog
class HyperLogLogCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return HyperLogLog(p=ACCURACY)  # p is used to control accuracy, higher is more accuracy

    def add_input(self, accumulator, element):
        tags = element['tags'].split('|') if element[
            'tags'] else []  # Split the tags if there are multiple separated by '|'
        for tag in tags:
            accumulator.update(tag.encode('utf-8'))  # Encode tag as bytes
        return accumulator

    def merge_accumulators(self, accumulators):
        merged_hll = HyperLogLog(p=ACCURACY)
        for hll in accumulators:
            merged_hll.merge(hll)
        return merged_hll

    def extract_output(self, accumulator):
        return accumulator.count()


def process_pubsub_message(element):
    message_data = json.loads(element.decode('utf-8'))
    return message_data


def run(argv=None):
    # Create a Pipeline using Apache Beam
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline = beam.Pipeline(options=pipeline_options)

    # Read from Pub/Sub and process the messages
    messages = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
        subscription='projects/stream-processing-384807/subscriptions/stack-overflow-sub')
            | 'Process Pub/Sub Messages' >> beam.Map(process_pubsub_message)
    )

    # Apply the HyperLogLog transform
    total_distinct_count = (
            messages
            | beam.WindowInto(beam.window.FixedWindows(60 * 5),
                              accumulation_mode=AccumulationMode.ACCUMULATING)
            # Group the messages into 5-minute  windows
            | beam.CombineGlobally(HyperLogLogCombineFn()).without_defaults()
    )

    # Convert the data to rows for writing to BigQuery
    rows = total_distinct_count | beam.ParDo(AddWindowInfo())

    # Write the data to BigQuery
    table_schema = {
        'fields': [
            {'name': 'window_start', 'type': 'TIMESTAMP'},
            {'name': 'window_end', 'type': 'TIMESTAMP'},
            {'name': 'count', 'type': 'FLOAT64'}
        ]
    }

    write_to_bq = WriteToBigQuery(
        table=PROJECT_ID + ':' + DATASET_ID + '.' + TABLE_ID,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )

    rows | 'Write to BigQuery' >> write_to_bq

    # Execute the pipeline
    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()
