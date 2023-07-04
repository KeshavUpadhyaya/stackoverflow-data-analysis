import argparse
import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime
from apache_beam.transforms.util import AccumulationMode
from apache_beam.transforms.window import FixedWindows

def parse_json(element):
    return json.loads(element)

def add_key(element):
    return (element['creation_date'], element)

def logElement(element):
    #print('Test', element)
    return element

def run(argv=None):
    parser = argparse.ArgumentParser()
    v, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Sample JSON data as Python lists
        #json_data2 = [
        #    ('2023-07-03T22:00:00', {'id': '73808006', 'creation_date': '2023-07-03T22:00:00'}),
        #    ('2023-07-03T23:00:00', {'id': '73808006', 'creation_date': '2023-07-03T23:00:00'})
        #]

        # Convert Python list to a PCollection
        #json_pcoll2 = (
        #        pipeline
        #        | "Create JSON 2 PCollection" >> beam.Create(json_data2)
        #        | "Assign fixed windowasdf" >> beam.WindowInto(beam.window.FixedWindows(60 * 60),
         #                                              trigger=AfterWatermark(early=AfterProcessingTime(5)),
        #                                               allowed_lateness=10,
        #                                               accumulation_mode=AccumulationMode.ACCUMULATING)
        #)

        input_collection2 = (
                pipeline
                | 'Read from Pub/Sub1' >> beam.io.ReadFromPubSub(
            subscription='projects/streamproc-python-lab/subscriptions/TestTopic-sub',
            timestamp_attribute="creation_date")
                | 'Decode message1' >> beam.Map(lambda x: x.decode('utf-8'))
                | 'Parse JSON1' >> beam.Map(parse_json)
                | 'Add key1' >> beam.Map(add_key)
                | 'Assign fixed window1' >> beam.WindowInto(beam.window.FixedWindows(60 * 60),
                                                           trigger=AfterWatermark(early=AfterProcessingTime(5)),
                                                           allowed_lateness=10,
                                                           accumulation_mode=AccumulationMode.ACCUMULATING)
                | 'Group by timestamp1' >> beam.GroupByKey()
        )

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
        )

        plants = (({
            'userTopic': input_collection, 'testTopic': input_collection2
        })
                  | 'Merge' >> beam.CoGroupByKey()
                  | beam.Map(print))

    # Execute the pipeline
    result = pipeline.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()
