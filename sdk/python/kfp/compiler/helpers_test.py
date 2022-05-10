# Copyright 2022 The Kubeflow Authors
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

import os
import tempfile
import unittest

import yaml
from absl.testing import parameterized
from google.protobuf import json_format
from kfp.compiler import helpers
from kfp.pipeline_spec import pipeline_spec_pb2


class TestValidatePipelineName(parameterized.TestCase):

    @parameterized.parameters(
        {
            'pipeline_name': 'my-pipeline',
            'is_valid': True,
        },
        {
            'pipeline_name': 'p' * 128,
            'is_valid': True,
        },
        {
            'pipeline_name': 'p' * 129,
            'is_valid': False,
        },
        {
            'pipeline_name': 'my_pipeline',
            'is_valid': False,
        },
        {
            'pipeline_name': '-my-pipeline',
            'is_valid': False,
        },
        {
            'pipeline_name': 'My pipeline',
            'is_valid': False,
        },
    )
    def test(self, pipeline_name, is_valid):

        if is_valid:
            helpers.validate_pipeline_name(pipeline_name)
        else:
            with self.assertRaisesRegex(ValueError, 'Invalid pipeline name: '):
                helpers.validate_pipeline_name('my_pipeline')


def pipeline_spec_from_file(filepath: str) -> str:
    with open(filepath, 'r') as f:
        dictionary = yaml.safe_load(f)
    return json_format.ParseDict(dictionary, pipeline_spec_pb2.PipelineSpec())


class TestWriteIrToFile(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        pipeline_spec = pipeline_spec_pb2.PipelineSpec()
        pipeline_spec.pipeline_info.name = 'pipeline-name'
        cls.pipeline_spec = pipeline_spec

    def test_yaml(self):
        with tempfile.TemporaryDirectory() as tempdir:
            temp_filepath = os.path.join(tempdir, 'output.yaml')
            helpers.write_pipeline_spec_to_file(self.pipeline_spec,
                                                temp_filepath)
            actual = pipeline_spec_from_file(temp_filepath)
        self.assertEqual(actual, self.pipeline_spec)

    def test_yml(self):
        with tempfile.TemporaryDirectory() as tempdir:
            temp_filepath = os.path.join(tempdir, 'output.yml')
            helpers.write_pipeline_spec_to_file(self.pipeline_spec,
                                                temp_filepath)
            actual = pipeline_spec_from_file(temp_filepath)
        self.assertEqual(actual, self.pipeline_spec)

    def test_json(self):
        with tempfile.TemporaryDirectory() as tempdir, self.assertWarnsRegex(
                DeprecationWarning, r"Compiling to JSON is deprecated"):
            temp_filepath = os.path.join(tempdir, 'output.json')
            helpers.write_pipeline_spec_to_file(self.pipeline_spec,
                                                temp_filepath)
            actual = pipeline_spec_from_file(temp_filepath)
        self.assertEqual(actual, self.pipeline_spec)

    def test_incorrect_extension(self):
        with tempfile.TemporaryDirectory() as tempdir, self.assertRaisesRegex(
                ValueError, r'should end with "\.yaml"\.'):
            temp_filepath = os.path.join(tempdir, 'output.txt')
            helpers.write_pipeline_spec_to_file(self.pipeline_spec,
                                                temp_filepath)


if __name__ == '__main__':
    unittest.main()
