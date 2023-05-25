import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def log_row(row):
    logging.info(str(row))
    return row


def main(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)

    input_query = """
SELECT id, title, view_count FROM `bigquery-public-data.stackoverflow.posts_questions` 
where creation_date >= '2008-11-25' and creation_date <= '2008-11-27' 
LIMIT 1000
    """

    pipeline = beam.Pipeline(options=pipeline_options)

    read_data = (
            pipeline
            | 'ReadFromBigQuery' >> beam.io.ReadFromBigQuery(query=input_query, use_standard_sql=True)
            | "Write to file" >> beam.Map(log_row)
    )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
