import argparse
import logging
import re
import json

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

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

    read = p | 'Read' >> ReadFromPubSub(subscription=known_args.input)
    
    decoded = read | 'parse' >> (beam.ParDo(DecodeParseJSON()))
    
    # Convert Units of Temp -> F and Pressure -> psi
    def convert_data(data):
      temp = data.get("temperature")
      pressure = data.get("pressure")

      if temp is not None:
        data["temperature"] = temp * (9/5) + 32

      if pressure is not None:
        data["pressure"] = pressure/6.895

      return data

    def serialize(data):
      return json.dumps(data).encode('utf-8')

    process=(decoded 
        | 'filter' >> beam.Filter(lambda x: x is not None)
        | 'convert' >> beam.Map(convert_data))
    
    # save to pubsub
    output = process | 'serialize' >> beam.Map(serialize)
    output | 'publish' >> WriteToPubSub(topic=known_args.output)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
