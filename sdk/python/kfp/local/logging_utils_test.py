# Copyright 2023 The Kubeflow Authors
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
"""Tests for logging_utils.py."""

import io
import unittest
from unittest import mock

from kfp import dsl
from kfp.local import logging_utils


class TestIndentedPrint(unittest.TestCase):

    @mock.patch('sys.stdout', new_callable=io.StringIO)
    def test(self, mocked_stdout):
        with logging_utils.indented_print(num_spaces=6):
            print('foo should be indented')
        expected = '      foo should be indented\n'
        actual = mocked_stdout.getvalue()
        self.assertEqual(
            actual,
            expected,
        )


class TestColorText(unittest.TestCase):

    def test_cyan(self):

        actual = logging_utils.color_text(
            'text to color',
            logging_utils.Colors.CYAN,
        )
        expected = '\x1b[91mtext to color\x1b[0m'
        self.assertEqual(actual, expected)

    def test_cyan(self):

        actual = logging_utils.color_text(
            'text to color',
            logging_utils.Colors.RED,
        )
        expected = '\x1b[91mtext to color\x1b[0m'
        self.assertEqual(actual, expected)


class TestRenderArtifact(unittest.TestCase):

    def test_empty(self):
        actual = logging_utils.render_artifact(dsl.Artifact())
        expected = "Artifact(name='', uri='', metadata={})"
        self.assertEqual(actual, expected)

    def test_contains_value(self):
        actual = logging_utils.render_artifact(
            dsl.Model(
                name='my_artifact',
                uri='/local/foo/bar',
                metadata={
                    'dict_field': {
                        'baz': 'bat'
                    },
                    'float_field': 3.14
                }))
        expected = "Model(name='my_artifact', uri='/local/foo/bar', metadata={'dict_field': {'baz': 'bat'}, 'float_field': 3.14})"
        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
