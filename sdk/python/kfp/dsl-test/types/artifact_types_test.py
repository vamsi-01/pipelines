# Copyright 2021 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for kfp.components.types.artifact_types."""

import contextlib
import json
import os
import unittest

from kfp import dsl


class ArtifactsTest(unittest.TestCase):

    def test_complex_metrics(self):
        metrics = dsl.ClassificationMetrics()
        metrics.log_roc_data_point(threshold=0.1, tpr=98.2, fpr=96.2)
        metrics.log_roc_data_point(threshold=24.3, tpr=24.5, fpr=98.4)
        metrics.set_confusion_matrix_categories(['dog', 'cat', 'horses'])
        metrics.log_confusion_matrix_row('dog', [2, 6, 0])
        metrics.log_confusion_matrix_cell('cat', 'dog', 3)
        metrics.log_confusion_matrix_cell('horses', 'horses', 3)
        metrics.metadata['test'] = 1.0
        with open(
                os.path.join(
                    os.path.dirname(__file__), 'test_data',
                    'expected_io_types_classification_metrics.json')
        ) as json_file:
            expected_json = json.load(json_file)
            self.assertEqual(expected_json, metrics.metadata)

    def test_complex_metrics_bulk_loading(self):
        metrics = dsl.ClassificationMetrics()
        metrics.log_roc_curve(
            fpr=[85.1, 85.1, 85.1],
            tpr=[52.6, 52.6, 52.6],
            threshold=[53.6, 53.6, 53.6])
        metrics.log_confusion_matrix(['dog', 'cat', 'horses'],
                                     [[2, 6, 0], [3, 5, 6], [5, 7, 8]])
        with open(
                os.path.join(
                    os.path.dirname(__file__), 'test_data',
                    'expected_io_types_bulk_load_classification_metrics.json')
        ) as json_file:
            expected_json = json.load(json_file)
            self.assertEqual(expected_json, metrics.metadata)


@contextlib.contextmanager
def set_temporary_env_var(key: str, value: str) -> None:
    original_value = os.environ.get(key)
    os.environ[key] = value
    try:
        yield
    finally:
        # Restore the original environment variable or unset it if it didn't exist before
        if original_value is None:
            del os.environ[key]
        else:
            os.environ[key] = original_value


class TestGetUri(unittest.TestCase):

    def test_raise_if_no_env_var(self):

        with self.assertRaisesRegex(
                RuntimeError,
                r"'TASK_ROOT' environment variable not found\. Cannot use 'dsl\.get_uri'\."
        ):
            dsl.get_uri()

    def test_default(self):
        with set_temporary_env_var('TASK_ROOT', 's3://foo/bar'):
            self.assertEqual('s3://foo/bar/Output', dsl.get_uri())

    def test_trailing_task_root_trailing_slash(self):
        with set_temporary_env_var('TASK_ROOT', 's3://foo/bar/'):
            self.assertEqual('s3://foo/bar/Output', dsl.get_uri())

    def test_suffix_arg(self):
        with set_temporary_env_var('TASK_ROOT', 'gs://foo/bar'):
            self.assertEqual('gs://foo/bar/model', dsl.get_uri('model'))


if __name__ == '__main__':
    unittest.main()
