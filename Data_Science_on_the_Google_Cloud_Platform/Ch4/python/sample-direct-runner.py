#!/usr/bin/env

import apache_beam as beam
from apache_beam.io.textio import ReadFromText
from apache_beam.io.textio import WriteToText

# apache beam documentation
# https://beam.apache.org/documentation/programming-guide/#pipeline-io

# Reference guide
# https://beam.apache.org/releases/pydoc/2.11.0/

class PrintFn(beam.DoFn):
    def process(self, element):

        # Just print
        print element

def format(s):
    ( word, count ) = s

    return "word: {0} count: {1}".format(word.encode('utf-8'), count)

def run_pipeline(in_file, out_file):
    # Simple process for apache beam pipeline
    # https://beam.apache.org/releases/pydoc/2.0.0/apache_beam.html
    with beam.Pipeline(runner='DirectRunner') as p:
        #
        # Pipeline(0)
        #
        # "lines" will include pcollections of each line

        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.ReadFromText
        lines = p | ReadFromText( file_pattern = in_file )

        #
        # Pipeline(n)
        # 1. Split each line with delimiter whitespace. Then convert each words into a pcollection element.
        # 2. Pair each words with '1'. Return with a tuple
        #
        output = (
            lines 
            | 'Split'       >> ( beam.FlatMap( lambda x: x.split() ) ).with_output_types(unicode)
            | 'PairWithOne' >> beam.Map( lambda x: (x, 1) )
            | 'CountByKey'  >> beam.CombinePerKey( sum )
            | 'Format'      >> beam.Map(format)
        )
        #
        # Pipeline(Final)
        #
        # Write the result to a file
        # https://beam.apache.org/releases/pydoc/2.11.0/apache_beam.io.textio.html#apache_beam.io.textio.WriteToText
        output | WriteToText( file_path_prefix = out_file )

def main():
    in_file = "/tmp/messages"
    out_file = "/tmp/messages.counted"

    run_pipeline(in_file, out_file)

if __name__ == "__main__":
    main()
