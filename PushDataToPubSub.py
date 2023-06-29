import argparse
from sys import argv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import WriteToPubSub

# Define the data that you want to publish
messages = [
    'Message 1',
    'Message 2',
    'Message 3',
    # Add more messages as needed
]

def run(argv=None):
    parser = argparse.ArgumentParser()
    v,pipleline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipleline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
                pipeline
                | 'Create messages' >> beam.Create(messages)
                | 'Publish to Pub/Sub' >> WriteToPubSub(topic='projects/streamproc-python-lab/topics/AnswerTopic')
        )

if __name__ == '__main__':
    run()