import argparse
import logging
import re
import json

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

# Decodes the Pub/Sub message and parses the JSON data into a dict
class DecodeParseJSON(beam.DoFn):
  def process(self, data):
    temp = data.decode('utf-8')
    try:
      yield json.loads(temp)
    except Exception:
      yield None

def run(argv=None, save_main_session=True):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--input',
      dest='input',
      default="projects/project-milestones-485816/subscriptions/mnist_image-sub",
      help='Input file to process.')
  parser.add_argument(
      '--output',
      dest='output',
      default="projects/project-milestones-485816/topics/mnist_predict",
      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

  with beam.Pipeline(options=pipeline_options) as p:

    # reads from the Google Topic as a subscription to ensure
    # all messages are consumed, not just new ones.
    read = p | 'Read' >> ReadFromPubSub(subscription=known_args.input)
    
    # Decode the Pub/Sub message and parse the JSON data into a dict
    decoded = read | 'parse' >> (beam.ParDo(DecodeParseJSON()))
    
    # Convert Units of Temp -> F and Pressure -> psi
    def convert_data(data):
      # Check if the entries are there
      temp = data.get("temperature")
      pressure = data.get("pressure")

      # If the entries exist, then convert them.
      if temp is not None:
        data["temperature"] = temp * (9/5) + 32

      if pressure is not None:
        data["pressure"] = pressure/6.895

      # return converted data
      return data

    # serialize the dictionary to JSON then binary
    def serialize(data):
      return json.dumps(data).encode('utf-8')

    # process data. Filter null values then run convert fucntion
    process=(decoded 
        | 'filter' >> beam.Filter(lambda x: x is not None)
        | 'convert' >> beam.Map(convert_data))
    
    # run serialize function then publish to the output topic
    output = process | 'serialize' >> beam.Map(serialize)
    output | 'publish' >> WriteToPubSub(topic=known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
