import json
import re
import logging
import argparse

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class WordExtractingDoFn(beam.DoFn):
    def __init__(self):
        super(WordExtractingDoFn, self).__init__()
        self.word_counter = Metrics.counter(self.__class__, 'words')
        self.word_length_counter = Metrics.counter(self.__class__, 'words_length')
        self.word_length_dist = Metrics.distribution(self.__class__, 'words_len_dist')
        self.empty_line_counter = Metrics.counter(self.__class__, 'empty_lines')

    def process(self, element):
        try:
            if not element:
                self.empty_line_counter.inc(1)
                return []
            else:
                data = json.loads(element)['reviewText']
                words = re.findall(r"[a-zA-Z\'']+", data)
                for word in words:
                    self.word_counter.inc()
                    self.word_length_counter.inc(len(word))
                    self.word_length_dist.update(len(word))
                return words
        except BaseException:
            return []

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default="gs://dataflow-ntk-super/data/reviews_books_1.json")
    parser.add_argument("--output", default="gs://dataflow-ntk-super/output")

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline = beam.Pipeline(options=pipeline_options)

    lines = pipeline | 'read' >> ReadFromText(known_args.input)
    counts = (lines
              | "split" >> (beam.ParDo(WordExtractingDoFn()).with_output_types(unicode))
              | "pair_with_one" >> (beam.Map(lambda word: (word, 1)))
              | "group" >> (beam.GroupByKey())
              | "count" >> (beam.Map(lambda (word, ones): (word, sum(ones)))))

    output = (counts | "format" >> beam.Map(lambda (word, total): "%s: %s" % (word, total)))

    output | "write" >> WriteToText(known_args.output)

    result = pipeline.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
