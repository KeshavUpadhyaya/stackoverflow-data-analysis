import argparse

import apache_beam as beam
import json

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode

PROJECT_ID = 'streamProc-Python-lab'
DATASET_ID = 'StreamProc_Data'
TABLE_ID = 'HourlyPostData'

class AddWindowInfo(beam.DoFn):
    def process(self, x, window=beam.DoFn.WindowParam):
        d = {}
        print('x:', x)
        d['TS'] = x[0]
        d['Count'] = x[1]
        d["window_start"] = window.start.to_utc_datetime()
        d["window_end"] = window.end.to_utc_datetime()
        print("object for writing to bigquery:", d)
        yield d

def parse_json(element):
    print(element)
    return json.loads(element)

def parse_bigquery(element):
    d = {}
    d['TS'] = element[0]
    d['Count'] = element[1]

def add_key(element):
    print((element['creation_date'], element))
    return (element['creation_date'], element)


def run(argv=None):

    parser = argparse.ArgumentParser()
    v, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    pipeline = beam.Pipeline(options=pipeline_options)

    table_schema = {
        'fields': [
            {'name': 'TS', 'type': 'TIMESTAMP'},
            {'name': 'Count', 'type': 'FLOAT64'},
            {'name': 'window_start', 'type': 'TIMESTAMP'},
            {'name': 'window_end', 'type': 'TIMESTAMP'}
        ]
    }

    input_collection = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
        subscription='projects/streamproc-python-lab/subscriptions/TestTopic-sub')
            | 'Decode message' >> beam.Map(lambda x: x.decode('utf-8'))
            | 'Parse JSON' >> beam.Map(parse_json)
            | 'Add key' >> beam.Map(add_key)
            | 'Assign fixed window' >> beam.WindowInto(beam.window.FixedWindows(60 * 60),
                                                         trigger=AfterWatermark(early=AfterProcessingTime(15)),
                                                         allowed_lateness=10,
                                                         accumulation_mode=AccumulationMode.ACCUMULATING)
            | 'Group by timestamp' >> beam.GroupByKey()
            | 'Count occurrences' >> beam.Map(lambda element: (element[0], len(element[1])))
            | 'Print to console' >> beam.Map(print)
            #| beam.ParDo(AddWindowInfo())
            #| beam.io.WriteToBigQuery(table='streamproc-python-lab.StreamProc_Data.HourlyPostData',
            #                          write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            #                          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )

    # Convert the data to rows for writing to BigQuery
    #rows = input_collection | beam.ParDo(AddWindowInfo())

    # Write the data to BigQuery


    #
    #write_to_bq = WriteToBigQuery(
     #   table='streamproc-python-lab.StreamProc_Data.HourlyPostData',
    ##    schema=table_schema,
    #    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
    #    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    #)

    #rows | 'Write to BigQuery' >> write_to_bq



    # Execute the pipeline
    result = pipeline.run()
    result.wait_until_finish()



if __name__ == '__main__':
    run()
