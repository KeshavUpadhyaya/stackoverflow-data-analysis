import argparse
import json
from datetime import datetime

import apache_beam as beam
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import TimestampedValue


# assumption that data looks as follows: "creation_date,other_field_1,other_field_2,..."
class ProcessMessages(beam.DoFn):
    def process(element):
        # Decode the PubSub message from bytes to string, and split the string
        message = element.decode('utf-8')
        print(message)
        # Extract the creation_date from the split message and convert to datetime
        creation_date = datetime.strptime(message[0], "%Y-%m-%d %H:%M:%S")

        # Emit the creation_date as the timestamp and the entire message as the value
        yield TimestampedValue(message, creation_date.timestamp())


def process_pubsub_message(element):
    message_data = json.loads(element.decode('utf-8'))
    print(message_data)
    return message_data


def run(argv=None):
    parser = argparse.ArgumentParser()
    v,pipleline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipleline_args)

    # Create a pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read from PubSub
        pubsub_data = (
                pipeline
                | 'Read from PubSub' >> ReadFromPubSub(subscription="projects/streamproc-python-lab/subscriptions/streamProc-StackOverflow-sub")
                | 'Process messages' >> beam.Map(process_pubsub_message)
            #                | 'Window' >> beam.WindowInto(FixedWindows(60 * 60),  # 1 hour windows
            #                                              trigger=AfterWatermark(early=AfterProcessingTime(1 * 60)),
            #                                              # Fire early every minute
            #                                              accumulation_mode=AccumulationMode.ACCUMULATING)  # Include late data in the next result
            #                | 'Count messages per window' >> Count.PerElement()
        )


# The data list now contains the grouped data.
# We'll plot a histogram of the number of elements per group (i.e., per window).
# timestamps, counts = zip(*data)

# fig, ax = plt.subplots()
# ax.bar(timestamps, counts)
# plt.xticks(rotation=90, fontsize=4)  # Rotate x-axis labels for readability
# plt.show()

if __name__ == '__main__':
    run()
