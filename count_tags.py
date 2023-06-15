import argparse
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datasketch import HyperLogLog

ACCURACY = 14

class AddWindowInfo(beam.DoFn):
    def process(self, x, window=beam.DoFn.WindowParam):
        d = {}
        d['count'] = x
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

    def without_defaults(self):
        return self


def process_pubsub_message(element):
    message_data = json.loads(element.decode('utf-8'))
    print(message_data)
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
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(subscription='projects/stream-processing-384807/subscriptions/stack-overflow-sub')
            | 'Process Pub/Sub Messages' >> beam.Map(process_pubsub_message)
    )

    # Apply the HyperLogLog transform
    total_distinct_count = (
            messages
            | beam.WindowInto(beam.window.FixedWindows(60))  # Group the messages into 1-minute windows
            | beam.CombineGlobally(HyperLogLogCombineFn()).without_defaults()
    )

    # Print the count of tags in each minute duration
    def print_count(element):
        print(element)

    total_distinct_count | beam.ParDo(AddWindowInfo())

    # Execute the pipeline
    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()
