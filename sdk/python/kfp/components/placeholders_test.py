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
"""Contains tests for kfp.components.placeholders."""
from absl.testing import parameterized
from kfp.components import placeholders


class TestExecutorInputPlaceholder(parameterized.TestCase):

    def test(self):
        self.assertEqual('{{$}}', placeholders.ExecutorInputPlaceholder())


class TestInputValuePlaceholder(parameterized.TestCase):

    def test(self):
        self.assertEqual("{{$.inputs.parameters['input1']}}",
                         placeholders.InputValuePlaceholder('input1'))


class TestInputPathPlaceholder(parameterized.TestCase):

    def test(self):
        self.assertEqual("{{$.inputs.artifacts['input1'].path}}",
                         placeholders.InputPathPlaceholder('input1'))


class TestInputUriPlaceholder(parameterized.TestCase):

    def test(self):
        self.assertEqual("{{$.inputs.artifacts['input1'].uri}}",
                         placeholders.InputUriPlaceholder('input1'))


class TestInputMetadataPlaceholder(parameterized.TestCase):

    def test(self):
        self.assertEqual("{{$.inputs.artifacts['input1'].metadata}}",
                         placeholders.InputMetadataPlaceholder('input1'))


class TestOutputPathPlaceholder(parameterized.TestCase):

    def test(self):
        self.assertEqual("{{$.outputs.artifacts['output1'].path}}",
                         placeholders.OutputPathPlaceholder('output1'))


class TestOutputParameterPlaceholder(parameterized.TestCase):

    def test(self):
        self.assertEqual("{{$.outputs.parameters['output1'].output_file}}",
                         placeholders.OutputParameterPlaceholder('output1'))


class TestOutputUriPlaceholder(parameterized.TestCase):

    def test(self):
        self.assertEqual("{{$.outputs.artifacts['output1'].uri}}",
                         placeholders.OutputUriPlaceholder('output1'))


class TestOutputMetadataPlaceholder(parameterized.TestCase):

    def test(self):
        self.assertEqual("{{$.outputs.artifacts['output1'].metadata}}",
                         placeholders.OutputMetadataPlaceholder('output1'))


class TestIfPresentPlaceholderStructure(parameterized.TestCase):
    # TODO test with nested

    @parameterized.parameters([
        (placeholders.IfPresentPlaceholder(
            input_name='input1', then=['echo', 'hello']), {
                'IfPresent': {
                    'InputName': 'input1',
                    'Then': ['echo', 'hello']
                }
            }),
        (placeholders.IfPresentPlaceholder(
            input_name='input1',
            then=['echo', 'hello'],
            else_=['echo', 'goodbye']), {
                'IfPresent': {
                    'InputName': 'input1',
                    'Then': ['echo', 'hello'],
                    'Else': ['echo', 'goodbye']
                }
            }),
    ])
    def test(self, actual: str, expected: str):
        self.assertEqual(actual, expected)


class TestConcatPlaceholder(parameterized.TestCase):
    # TODO test with nested
    @parameterized.parameters([(placeholders.ConcatPlaceholder(['a', 'b', 'c']),
                                {
                                    'Concat': ['a', 'b', 'c']
                                })])
    def test(self, actual: str, expected: str):
        self.assertEqual(actual, expected)

    def test2(self):
        actual = placeholders.ConcatPlaceholder([
            'my-prefix-',
            placeholders.IfPresentPlaceholder(
                input_name='input1',
                then=[placeholders.ConcatPlaceholder(['infix-', 'input1'])])
        ])
        expected = """'{"Concat": ["my-prefix-", {"IfPresent": {"InputName": "input1", "Then":
          [{"Concat": ["infix-", "{{$.inputs.parameters[''input1'']}}"]}]}}]}'"""
        self.assertEqual(actual, expected)


# class TestProcessCommandArg(unittest.TestCase):

#     def test_string(self):
#         arg = 'test'
#         struct = placeholders.maybe_convert_placeholder_string_to_placeholder(
#             arg)
#         self.assertEqual(struct, arg)

#     def test_input_value_placeholder(self):
#         arg = "{{$.inputs.parameters['input1']}}"
#         actual = placeholders.maybe_convert_placeholder_string_to_placeholder(
#             arg)
#         expected = placeholders.InputValuePlaceholder(input_name='input1')
#         self.assertEqual(actual, expected)

#     def test_input_path_placeholder(self):
#         arg = "{{$.inputs.artifacts['input1'].path}}"
#         actual = placeholders.maybe_convert_placeholder_string_to_placeholder(
#             arg)
#         expected = placeholders.InputPathPlaceholder('input1')
#         self.assertEqual(actual, expected)

#     def test_input_uri_placeholder(self):
#         arg = "{{$.inputs.artifacts['input1'].uri}}"
#         actual = placeholders.maybe_convert_placeholder_string_to_placeholder(
#             arg)
#         expected = placeholders.InputUriPlaceholder('input1')
#         self.assertEqual(actual, expected)

#     def test_output_path_placeholder(self):
#         arg = "{{$.outputs.artifacts['output1'].path}}"
#         actual = placeholders.maybe_convert_placeholder_string_to_placeholder(
#             arg)
#         expected = placeholders.OutputPathPlaceholder('output1')
#         self.assertEqual(actual, expected)

#     def test_output_uri_placeholder(self):
#         placeholder = "{{$.outputs.artifacts['output1'].uri}}"
#         actual = placeholders.maybe_convert_placeholder_string_to_placeholder(
#             placeholder)
#         expected = placeholders.OutputUriPlaceholder('output1')
#         self.assertEqual(actual, expected)

#     def test_output_parameter_placeholder(self):
#         placeholder = "{{$.outputs.parameters['output1'].output_file}}"
#         actual = placeholders.maybe_convert_placeholder_string_to_placeholder(
#             placeholder)
#         expected = placeholders.OutputParameterPlaceholder('output1')
#         self.assertEqual(actual, expected)

#     def test_concat_placeholder(self):
#         placeholder = "{{$.inputs.parameters[''input1'']}}+{{$.inputs.parameters[''input2'']}}"
#         actual = placeholders.maybe_convert_placeholder_string_to_placeholder(
#             placeholder)
#         expected = placeholders.ConcatPlaceholder(items=[
#             placeholders.InputValuePlaceholder(input_name='input1'),
#             placeholders.InputValuePlaceholder(input_name='input2')
#         ])
#         self.assertEqual(actual, expected)
