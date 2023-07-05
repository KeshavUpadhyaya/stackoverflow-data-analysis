import argparse

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from datasketch import HyperLogLog

ACCURACY = 14


# Custom combine function for HyperLogLog
class HyperLogLogCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return HyperLogLog(p=ACCURACY)  # p is used to control accuracy, higher is more accuracy

    def add_input(self, accumulator, element):
        tags = element['tags'].split('|') if element[
            'tags'] else []  # Split the tags if there are multiple separated by '|'
        for tag in tags:
            accumulator.update(tag.encode('utf-8'))  # Encode tag as bytes
        return accumulator

    def merge_accumulators(self, accumulators):
        merged_hll = HyperLogLog(p=ACCURACY)
        for hll in accumulators:
            merged_hll.merge(hll)
        return merged_hll

    def extract_output(self, accumulator):
        return accumulator.count()

def run(argv=None):
    # Define the query to retrieve the data
    query = f"""
        SELECT tags
        FROM `bigquery-public-data.stackoverflow.posts_questions`
        WHERE EXTRACT(YEAR FROM creation_date) BETWEEN 2008 AND 2009
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

    # Apply the HyperLogLog transform
    total_distinct_count = (
        data
        | 'Compute Unique Tags (HyperLogLog)' >> beam.CombineGlobally(HyperLogLogCombineFn())
    )

    # Output the total count of unique tags using HyperLogLog estimate
    total_distinct_count | 'Print HyperLogLog Estimate' >> beam.Map(print)

    # Compute the actual total number of unique tags
    actual_distinct_count = (
        data
        | 'Split Tags' >> beam.FlatMap(lambda element: element['tags'].split('|') if element['tags'] else [])
        | 'Remove Duplicates' >> beam.Distinct()
        | 'Count Unique Tags' >> beam.combiners.Count.Globally()
    )

    # Output the actual total count of unique tags
    actual_distinct_count | 'Print Actual Unique Count' >> beam.Map(print)

    # Execute the pipeline
    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()
