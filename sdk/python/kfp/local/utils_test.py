# Copyright 2023 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License")
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
"""Test for utilities."""
import unittest

from google.protobuf import json_format
from kfp.local import utils
from kfp.pipeline_spec import pipeline_spec_pb2

executor_input = pipeline_spec_pb2.ExecutorInput()
dict_executor_input = {
    'inputs': {
        'artifacts': {
            'in_art': {
                'artifacts': [{
                    'metadata': {},
                    'name': 'in_art',
                    'type': {
                        'schemaTitle': 'system.Artifact'
                    },
                    'uri': '/foo/bar/in_art'
                }]
            }
        },
        'parameterValues': {
            'in_param': 'text'
        },
        'parameters': {
            'in_param': {
                'stringValue': 'text'
            }
        }
    },
    'outputs': {
        'artifacts': {
            'out_art': {
                'artifacts': [{
                    'metadata': {},
                    'name': 'out_art',
                    'type': {
                        'schemaTitle': 'system.Artifact'
                    },
                    'uri': '/foo/bar/out_art'
                }]
            }
        },
        'outputFile': '/foo/bar/executor_output.json',
        'parameters': {
            'Output': {
                'outputFile': '/foo/bar/Output'
            }
        }
    }
}
json_format.ParseDict(dict_executor_input, executor_input)


class TestReplacePlaceholderConstants(unittest.TestCase):

    def test_executor_input(self):
        ...


class TestReplacePlaceholdersForElement(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.executor_input = executor_input

    def test_executor_input(self):
        actual = utils.replace_placholder('{{$}}', self.executor_input)
        expected = self.executor_input
        self.assertEqual(actual, json_format.MessageToJson(self.executor_input))

    def test_input_parameter(self):
        actual = utils.replace_placholder(
            "{{$.inputs.parameters[''in_param'']}}", self.executor_input)
        expected = self.executor_input
        self.assertEqual(
            actual,
            json_format.MessageToJson(
                self.executor_input.inputs.parameters['in_param']))


r'{{$}}'
r'{{$.outputs.output_file}}'
r"{{$.inputs.parameters['$0']}}"
r"{{$.inputs.artifacts['$0']}}"
r"{{$.inputs.artifacts['$0'].uri}}"
r"{{$.inputs.artifacts['$0'].path}}"
r"{{$.inputs.artifacts['$0'].value}}"
r"{{$.inputs.artifacts['$0'].properties['$1']}}"
r"{{$.inputs.artifacts['$0'].custom_properties['$1']}}"
r"{{$.inputs.artifacts['$0'].metadata}}"
r"{{$.inputs.artifacts['$0'].metadata['$1']}}"
r"{{$.outputs.artifacts['$0']}}"
r"{{$.outputs.artifacts['$0'].uri}}"
r"{{$.outputs.artifacts['$0'].path}}"
r"{{$.outputs.artifacts['$0'].properties['$1']}}"
r"{{$.outputs.artifacts['$0'].custom_properties['$1']}}"
r"{{$.outputs.artifacts['$0'].metadata}}"
r"{{$.outputs.artifacts['$0'].metadata['$1']}}"
r"{{$.outputs.parameters['$0'].output_file}}"


class TestReplacePlaceholders(unittest.TestCase):
    ...


class TestMakeArtifactList(unittest.TestCase):
    ...


class TestConstructExecutorInput(unittest.TestCase):
    ...
