import json
import re
import logging
import argparse
import hashlib
import msgpack

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

class Document(object, beam.coders.Coder):
    def __init__(self, document_id, review_words, word_length, document_length):
        self.document_id = document_id
        self.review_words = review_words
        self.word_length = word_length
        self.document_length = document_length

    def encode(self, document):
        return msgpack.dumps([
            document.document_id,
            document.review_words,
            document.word_length,
            document.document_length
        ])

    def decode(self, data):
        return Document(*msgpack.loads(data))

    def is_deterministic(self):
        return True

class ExtractWordDoFn(beam.DoFn):
    def __init__(self):
        super(ExtractWordDoFn, self).__init__()
        self.word_counter = Metrics.counter(self.__class__, 'words')
        self.word_length_counter = Metrics.counter(self.__class__, 'words_length')
        self.word_length_dist = Metrics.distribution(self.__class__, 'words_len_dist')
        self.empty_line_counter = Metrics.counter(self.__class__, 'empty_lines')
        self.error_counter = Metrics.counter(self.__class__, "Parsing error")

    def process(self, element):
        try:
            if not element:
                self.empty_line_counter.inc(1)
                return []
            else:
                data = json.loads(element)
                review_text = data["reviewText"].lower()
                hashable_text = re.sub(r"[^\x00-\x7f]+", "", review_text)
                hash_function = hashlib.sha1(hashable_text)
                hash_function.update(str(data.get("unixReviewTime", "")))
                hash_function.update(data.get("asin", ""))
                document_id = hash_function.hexdigest()
                review_words = re.findall(r"\b[\w\']+\b", review_text)
                self.word_counter.inc(len(review_words))
                for word in review_words:
                    self.word_length_counter.inc(len(word))
                    self.word_length_dist.update(len(word))
                return [Document(document_id, review_words, len(review_words), len(review_text))]
        except BaseException:
            self.error_counter.inc()
            return []

class FlattenFn(beam.DoFn):
    def __init__(self):
        super(FlattenFn, self).__init__()
        self.instance_counter = Metrics.counter(self.__class__, 'invalid_instance_counter')

    def process(self, element):
        if not isinstance(element, Document):
            self.instance_counter.inc()
            return []

        return [(word, element.document_id) for word in element.review_words]

class CalcFreqFn(beam.DoFn):
    def __init__(self):
        super(CalcFreqFn, self).__init__()
        self.error_counter = Metrics.counter(self.__class__, 'calc_error_counter')

    def process(self, element):
        try:
            word = element[0]
            total_freq = float(element[1][0][0])
            return [(document_id, (word, count / total_freq)) for document_id, count in element[1][1]]
        except BaseException:
            self.error_counter.inc()
            return []

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default="gs://dataflow-ntk-super/data/*")
    parser.add_argument("--output", default="gs://dataflow-ntk-super/output/")

    known_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline = beam.Pipeline(options=pipeline_options)

    lines = pipeline | 'Read From Cloud Storage' >> ReadFromText(known_args.input)
    documents = lines | "Parse Data" >> (beam.ParDo(ExtractWordDoFn()).with_output_types(Document))

    word_counts = (
        documents
        | "Extract Word" >> (beam.ParDo(lambda doc: doc.review_words))
        | "Pair of One" >> (beam.Map(lambda word: (word, 1)))
        | "Combine to Count Words" >> (beam.CombinePerKey(sum))
    )

    word_count_by_document = (
        documents
        | "Word Document Pairing" >> (beam.ParDo(FlattenFn()))
        | "Pair of One for Document" >> (beam.Map(lambda doc_word: (doc_word, 1)))
        | "Count of Word by Document" >> (beam.CombinePerKey(sum))
        | "Rearange" >> (beam.Map(lambda data: (data[0][0], (data[0][1], data[1]))))
    )

    word_freq = (
        (word_counts, word_count_by_document)
        | "Joining Word" >> beam.CoGroupByKey()
        | "Remaping" >> beam.ParDo(CalcFreqFn())
    )

    output = (word_freq | "Format" >> (beam.Map(json.dumps)))

    _ = output | "Write" >> WriteToText(known_args.output)

    result = pipeline.run()
    result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
