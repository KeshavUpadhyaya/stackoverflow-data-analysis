import argparse

import apache_beam as beam
import json

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
from apache_beam.transforms.window import FixedWindows
from datetime import timedelta


def parse_json(element):
    print(element)
    return json.loads(element)


def add_key(element):
    return (element['creation_date'], element)


def run(argv=None):
    parser = argparse.ArgumentParser()
    v, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        input_collection = (
                pipeline
                | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
            subscription='projects/streamproc-python-lab/subscriptions/UserTopic-sub',
            timestamp_attribute="creation_date")
                | 'Decode message' >> beam.Map(lambda x: x.decode('utf-8'))
                | 'Parse JSON' >> beam.Map(parse_json)
                | 'Add key' >> beam.Map(add_key)
                | 'Assign fixed window' >> beam.WindowInto(beam.window.FixedWindows(60 * 60),
                                                             trigger=AfterWatermark(early=AfterProcessingTime(5)),
                                                             allowed_lateness=10,
                                                             accumulation_mode=AccumulationMode.ACCUMULATING)
                | 'Group by timestamp' >> beam.GroupByKey()
                | 'Count occurrences' >> beam.Map(lambda element: (element[0], len(element[1])))
                | 'Print to console' >> beam.Map(print)
        )

    # Execute the pipeline
    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()
