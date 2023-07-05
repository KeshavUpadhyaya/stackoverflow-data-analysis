import argparse
from sys import argv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import WriteToPubSub

data_User = [
    '''{"id": "4697948","location": "null","reputation": "13","up_votes": "0","down_votes": "0"}''',
    '''{"id": "15402852","location": "Tuscaloosa, AL, USA","reputation": "13","up_votes": "0","down_votes": "0"}''',
    '''{"id": "11740355","location": "null","reputation": "13","up_votes": "0","down_votes": "0"}''',
    # Add more messages as needed
]

data = [
    '{"id": "70882985", "creation_date": "2023-07-03T16:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T16:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T16:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T16:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T16:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T16:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T17:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T17:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T17:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T17:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T17:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T17:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T18:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T18:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T18:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T18:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T19:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T19:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T19:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T19:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T19:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T19:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T19:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T19:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T19:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T20:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "70882985", "creation_date": "2023-07-03T21:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T22:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
    '{"id": "73808006", "creation_date": "2023-07-03T23:00:00"}',
]

data_posts = [
    '{"id": "70882985"}',
    '{"id": "13742944"}',
    '{"id": "57390275"}',
    '{"id": "47598432"}',
    '{"id": "58948934"}',
]

data_votes = [
    '{"post_id": "70882985", "vote_type_id": "1"}',
    '{"post_id": "70882985", "vote_type_id": "2"}',
    '{"post_id": "70882985", "vote_type_id": "2"}',
    '{"post_id": "70882985", "vote_type_id": "2"}',
    '{"post_id": "70882985", "vote_type_id": "2"}',
    '{"post_id": "70882985", "vote_type_id": "1"}',
    '{"post_id": "13742944", "vote_type_id": "1"}',
    '{"post_id": "13742944", "vote_type_id": "2"}',
    '{"post_id": "13742944", "vote_type_id": "1"}',
    '{"post_id": "13742944", "vote_type_id": "2"}',
    '{"post_id": "57390275", "vote_type_id": "2"}',
    '{"post_id": "57390275", "vote_type_id": "1"}',
    '{"post_id": "57390275", "vote_type_id": "2"}',
    '{"post_id": "57390275", "vote_type_id": "2"}',
    '{"post_id": "47598432", "vote_type_id": "1"}',
    '{"post_id": "47598432", "vote_type_id": "1"}',
    '{"post_id": "47598432", "vote_type_id": "2"}',
    '{"post_id": "58948934", "vote_type_id": "2"}',
    '{"post_id": "58948934", "vote_type_id": "1"}',
    '{"post_id": "58948934", "vote_type_id": "2"}',
]


topic = 'projects/streamproc-python-lab/topics/TestTopic'
#topic = 'projects/streamproc-python-lab/topics/VoteTopic'
#topic = 'projects/streamproc-python-lab/topics/UserTopic'
def run(argv=None):
    parser = argparse.ArgumentParser()
    v, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | 'Create messages' >> beam.Create(data)
            #| 'Create messages' >> beam.Create(data_posts)
            #| 'Create messages' >> beam.Create(data_votes)
            | 'Encode as bytestrings' >> beam.Map(lambda x: x.encode('utf-8'))
            | 'Publish to Pub/Sub' >> WriteToPubSub(topic=topic)
        )

if __name__ == '__main__':
    run()
