import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datasketch import HyperLogLog


def run(argv=None):
    table = 'bigquery-public-data.stackoverflow.posts_questions'
    # Define the query to retrieve the data
    query = f"""
        SELECT tags
        FROM `{table}`
        WHERE EXTRACT(YEAR FROM creation_date) BETWEEN 2019 AND 2021
        LIMIT 1000
    """

    # Create a Pipeline using Apache Beam
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline = beam.Pipeline(options=pipeline_options)

    data = (
            pipeline
            | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)
    )

    # Execute the pipeline
    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()
