# Copyright 2016 The Oppia Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS-IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Performance tests for string classifier.

Run this script from the Oppia root directory:

    export PYTHONPATH=$PYTHONPATH:.
    python core/tests/performance_tests/string_classifier_performance_test.py

"""

import os
import time
import yaml

from core.domain.classifier_services import StringClassifier

def measure_runtime(func):
    """A decorator that measures the amount of time func takes to run.

    Args:
        func: function. The function to be measured.

    Returns:
        function. The decorated function by this decorator.
    """
    def time_taken(obj, num):
        """Prints the time taken by func to run a given number of samples.

        Args:
            obj: object. The class instance func belongs to.
            num: int. The number of samples to run with.

        Returns:
            Result of func.
        """
        start = time.time()
        result = func(obj, num)
        end = time.time()
        print '%s spent %f seconds for %d instances' % (func.__name__,
                                                        end - start, num)
        return result
    return time_taken

class StringClassifierBenchmarker(object):
    """Benchmark for string classifier.

    Usage:
        benchmark = StringClassifierBenchmarker()
        benchmark.generate_training_benchmarks()
        benchmark.generate_prediction_benchmarks()

    Attributes:
        examples: list of two-element lists. An item represents a sample
            in the form of [doc, label].
        docs_to_classify: list(str). A list of docs to be classified.
        classifier_model_dict: dict. The model generated by train().
    """
    def __init__(self):
        """Initializes StringClassifierBenchmark by parsing the data
        file containing samples.
        """
        yaml_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 '../data/string_classifier_test.yaml')
        doc_to_label = {}
        with open(yaml_path, 'r') as yaml_file:
            yaml_dict = yaml.load(yaml_file)
            interactions = yaml_dict['states']['Home']['interaction']
            # The first element contains no training data,
            # so only consider [1:].
            for answer_group in interactions['answer_groups'][1:]:
                label = answer_group['outcome']['feedback'][0]
                for rule in answer_group['rule_specs']:
                    if 'inputs' in rule and 'training_data' in rule['inputs']:
                        for doc in rule['inputs']['training_data']:
                            if doc not in doc_to_label:
                                doc_to_label[doc] = []
                            doc_to_label[doc].append(label)
        self.examples = [[doc, doc_to_label[doc]] for doc in doc_to_label]
        self.docs_to_classify = [doc[0] for doc in self.examples]
        self.classifier_model_dict = None

    @measure_runtime
    def train(self, num):
        """Trains a model with num samples.

        Args:
            num: int. The number of samples to run with.

        Returns:
            dict. The dict representing the resulting classifier model.
        """
        string_classifier = StringClassifier()
        string_classifier.load_examples(self.examples[:num])
        classifier_dict = string_classifier.to_dict()
        return classifier_dict

    def generate_training_benchmarks(self):
        """Conduct benchmarking on model training at a pace of adding
        100 samples each time.
        """
        for num in xrange(100, len(self.examples), 100):
            self.train(num)

    @measure_runtime
    def predict(self, num):
        """Predicts the label for num samples with self.classifier.

        Args:
            num: int. The number of samples to predict the label for.
        """
        if not self.classifier_model_dict:
            raise Exception('No classifier found')
        string_classifier = StringClassifier()
        string_classifier.from_dict(self.classifier_model_dict)
        doc_ids = string_classifier.add_docs_for_predicting(
            self.docs_to_classify[:num])
        for doc_id in doc_ids:
            string_classifier.predict_label_for_doc(doc_id)

    def generate_prediction_benchmarks(self):
        """Conduct benchmarking on predicting with self.classifier
        at a pace of adding 100 samples each time.
        """
        self.classifier_model_dict = self.train(len(self.examples))
        for num in xrange(100, len(self.docs_to_classify), 100):
            self.predict(num)


def main():
    benchmarker = StringClassifierBenchmarker()
    benchmarker.generate_training_benchmarks()
    benchmarker.generate_prediction_benchmarks()

if __name__ == '__main__':
    main()
