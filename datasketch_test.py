import apache_beam as beam
from datasketch import HyperLogLog

# Sample input data
data = [
    {'tags': 'apple'},
    {'tags': 'apple|banana'}
]


# Custom combine function for HyperLogLog
class HyperLogLogCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return HyperLogLog()

    def add_input(self, accumulator, element):
        tags = element.split('|')  # Split the tags if there are multiple separated by '|'
        for tag in tags:
            accumulator.update(tag.encode('utf-8'))  # Encode tag as bytes
        return accumulator

    def merge_accumulators(self, accumulators):
        merged_hll = HyperLogLog()
        for hll in accumulators:
            merged_hll.merge(hll)
        return merged_hll

    def extract_output(self, accumulator):
        return accumulator.count()


# Pipeline setup
with beam.Pipeline() as p:
    # Create a PCollection from the input data
    input_data = p | beam.Create(data)

    # Apply the HyperLogLog transform
    total_distinct_count = (
            input_data
            | beam.Map(lambda element: element['tags'])
            | beam.CombineGlobally(HyperLogLogCombineFn())
    )

    # Output the total count of unique tags
    total_distinct_count | beam.Map(print)
