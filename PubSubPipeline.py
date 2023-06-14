import apache_beam as beam
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import collections
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.transforms.window import FixedWindows, TimestampedValue
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
from apache_beam.transforms.combiners import Count

#assumption that data looks as follows: "creation_date,other_field_1,other_field_2,..."
class ProcessMessages(beam.DoFn):
    def process(self, element):
        # Decode the PubSub message from bytes to string, and split the string
        message = element.decode('utf-8').split(',')

        # Extract the creation_date from the split message and convert to datetime
        creation_date = datetime.strptime(message[0], "%Y-%m-%d %H:%M:%S")

        # Emit the creation_date as the timestamp and the entire message as the value
        yield TimestampedValue(message, creation_date.timestamp())

# Replace with your own project and subscription details
project_id = 'your-project-id'
subscription_id = 'your-subscription-id'
topic = f'projects/{project_id}/topics/{subscription_id}'

# Define pipeline options
pipeline_options = PipelineOptions(
    project=project_id,
    runner='DirectRunner'  # Use DirectRunner for running the pipeline locally
)

data = []

# Create a pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    # Read from PubSub
    pubsub_data = (
            pipeline
            | 'Read from PubSub' >> ReadFromPubSub(topic=topic)
            | 'Process messages' >> beam.ParDo(ProcessMessages())
            | 'Window' >> beam.WindowInto(FixedWindows(60*60), # 1 hour windows
                                           trigger=AfterWatermark(early=AfterProcessingTime(1*60)), # Fire early every minute
                                           accumulation_mode=AccumulationMode.ACCUMULATING) # Include late data in the next result
            | 'Count messages per window' >> Count.PerElement()
            | "Store results" >> beam.Map(lambda x: data.append(x))
    )

# The data list now contains the grouped data.
# We'll plot a histogram of the number of elements per group (i.e., per window).
timestamps, counts = zip(*data)

fig, ax = plt.subplots()
ax.bar(timestamps, counts)
plt.xticks(rotation=90, fontsize=4)  # Rotate x-axis labels for readability
plt.show()
