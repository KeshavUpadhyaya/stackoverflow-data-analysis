from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam as beam
import json
from collections import Counter

# Define the Pipeline with options
options = PipelineOptions()
p = beam.Pipeline(options=options)

def log(element):
    print(element)
    return element

data_posts = (
        p
        | 'Read from Pub/Sub Posts' >> beam.io.ReadFromPubSub(
    subscription='projects/streamproc-python-lab/subscriptions/TestTopic-sub')
        | "Parse Posts" >> beam.Map(lambda x: (json.loads(x.decode('utf-8'))['id'], json.loads(x.decode('utf-8'))))
        | 'Assign votes to fixed window' >> beam.WindowInto(beam.window.FixedWindows(60 * 60),
                                                    trigger=AfterWatermark(early=AfterProcessingTime(10)),
                                                    allowed_lateness=100,
                                                    accumulation_mode=AccumulationMode.ACCUMULATING)
        | "Map data2" >> beam.Map(log)

)

data_votes = (
        p
        | 'Read from Pub/Sub Votes' >> beam.io.ReadFromPubSub(
    subscription='projects/streamproc-python-lab/subscriptions/VoteTopic-sub')
        | "Parse Votes" >> beam.Map(
    lambda x: (json.loads(x.decode('utf-8'))['post_id'], json.loads(x.decode('utf-8'))))
        | 'Assign posts to fixed window' >> beam.WindowInto(beam.window.FixedWindows(60 * 60),
                                                    trigger=AfterWatermark(early=AfterProcessingTime(10)),
                                                    allowed_lateness=100,
                                                    accumulation_mode=AccumulationMode.ACCUMULATING)
        | "Map data1" >> beam.Map(log)

)

joined_data = (
        {"posts": data_posts, "votes": data_votes}
        | "Join Data" >> beam.CoGroupByKey()
        | "Map data" >> beam.Map(log)
)

def format_votes(element):
    id, data = element
    vote_counts = Counter(vote['vote_type_id'] for vote in data['votes'])
    return id, vote_counts


vote_type_counts = joined_data | "Count Vote Types" >> beam.Map(format_votes)


# Print the results
def print_results(element):
    id, vote_counts = element
    print(f'"id": "{id}", {", ".join(f"Vote Type ID {vtid}: {count}" for vtid, count in vote_counts.items())}')


vote_type_counts | "Print Results" >> beam.Map(print_results)

# Run the Pipeline
result = p.run()
result.wait_until_finish()
