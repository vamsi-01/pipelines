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
"""Tests for LocalTask-related logic."""
import os
import tempfile
import unittest

from google.protobuf import json_format
from google.protobuf import message
from google.protobuf import struct_pb2
from kfp import dsl
from kfp.local import local_task
from kfp.pipeline_spec import pipeline_spec_pb2


def write_proto_to_json_file(
    proto_message: message.Message,
    file_path: str,
) -> None:
    """Utility for tests."""
    json_string = json_format.MessageToJson(proto_message)

    with open(file_path, 'w') as json_file:
        json_file.write(json_string)


class TestLoadExecutorOutput(unittest.TestCase):

    def test_exists(self):
        with tempfile.TemporaryDirectory() as tempdir:
            executor_output = pipeline_spec_pb2.ExecutorOutput(
                parameter_values={
                    'foo': struct_pb2.Value(string_value='foo_value')
                })
            path = os.path.join(tempdir, 'executor_output.json')
            write_proto_to_json_file(executor_output, path)

            result = local_task.load_executor_output(path)
            self.assertEqual(
                result.parameter_values['foo'],
                struct_pb2.Value(string_value='foo_value'),
            )

    def test_not_exists(self):
        non_existent_path = 'non_existent_path.json'

        with self.assertRaisesRegex(FileNotFoundError,
                                    r'No such file or directory:'):
            local_task.load_executor_output(non_existent_path)


class TestGetOutputsFromExecutorOutput(unittest.TestCase):

    def test(self):
        ...


class TestPb2ValueToPython(unittest.TestCase):

    def test_null(self):
        inp = struct_pb2.Value(null_value=struct_pb2.NullValue.NULL_VALUE)
        actual = local_task.pb2_value_to_python(inp)
        expected = None
        self.assertEqual(actual, expected)

    def test_string(self):
        inp = struct_pb2.Value(string_value='foo_value')
        actual = local_task.pb2_value_to_python(inp)
        expected = 'foo_value'
        self.assertEqual(actual, expected)

    def test_number_int(self):
        inp = struct_pb2.Value(number_value=1)
        actual = local_task.pb2_value_to_python(inp)
        expected = 1.0
        self.assertEqual(actual, expected)

    def test_number_float(self):
        inp = struct_pb2.Value(number_value=1.0)
        actual = local_task.pb2_value_to_python(inp)
        expected = 1.0
        self.assertEqual(actual, expected)

    def test_bool(self):
        inp = struct_pb2.Value(bool_value=True)
        actual = local_task.pb2_value_to_python(inp)
        expected = True
        self.assertIs(actual, expected)

    def test_dict(self):
        struct_value = struct_pb2.Struct()
        struct_value.fields['my_key'].string_value = 'my_value'
        struct_value.fields['other_key'].bool_value = True
        inp = struct_pb2.Value(struct_value=struct_value)
        actual = local_task.pb2_value_to_python(inp)
        expected = {'my_key': 'my_value', 'other_key': True}
        self.assertEqual(actual, expected)


class TestRuntimeArtifactToDslArtifact(unittest.TestCase):

    def test_artifact(self):
        metadata = struct_pb2.Struct()
        metadata.fields['foo'].string_value = 'bar'
        type_ = pipeline_spec_pb2.ArtifactTypeSchema(
            schema_title='system.Artifact',
            schema_version='0.0.1',
        )
        runtime_artifact = pipeline_spec_pb2.RuntimeArtifact(
            name='a',
            uri='gs://bucket/foo',
            metadata=metadata,
            type=type_,
        )
        actual = local_task.runtime_artifact_to_dsl_artifact(runtime_artifact)
        expected = dsl.Artifact(
            name='a',
            uri='gs://bucket/foo',
            metadata={'foo': 'bar'},
        )
        self.assertEqual(actual, expected)

    def test_dataset(self):
        metadata = struct_pb2.Struct()
        metadata.fields['baz'].string_value = 'bat'
        type_ = pipeline_spec_pb2.ArtifactTypeSchema(
            schema_title='system.Dataset',
            schema_version='0.0.1',
        )
        runtime_artifact = pipeline_spec_pb2.RuntimeArtifact(
            name='d',
            uri='gs://bucket/foo',
            metadata=metadata,
            type=type_,
        )
        actual = local_task.runtime_artifact_to_dsl_artifact(runtime_artifact)
        expected = dsl.Dataset(
            name='d',
            uri='gs://bucket/foo',
            metadata={'baz': 'bat'},
        )
        self.assertEqual(actual, expected)


class TestArtifactListToDslArtifact(unittest.TestCase):

    def test_not_list(self):
        metadata = struct_pb2.Struct()
        metadata.fields['foo'].string_value = 'bar'
        type_ = pipeline_spec_pb2.ArtifactTypeSchema(
            schema_title='system.Artifact',
            schema_version='0.0.1',
        )
        runtime_artifact = pipeline_spec_pb2.RuntimeArtifact(
            name='a',
            uri='gs://bucket/foo',
            metadata=metadata,
            type=type_,
        )
        artifact_list = pipeline_spec_pb2.ArtifactList(
            artifacts=[runtime_artifact])

        actual = local_task.artifact_list_to_dsl_artifact(
            artifact_list,
            is_artifact_list=False,
        )
        expected = dsl.Artifact(
            name='a',
            uri='gs://bucket/foo',
            metadata={'foo': 'bar'},
        )
        self.assertEqual(actual, expected)

    def test_single_entry_list(self):
        metadata = struct_pb2.Struct()
        metadata.fields['foo'].string_value = 'bar'
        type_ = pipeline_spec_pb2.ArtifactTypeSchema(
            schema_title='system.Dataset',
            schema_version='0.0.1',
        )
        runtime_artifact = pipeline_spec_pb2.RuntimeArtifact(
            name='a',
            uri='gs://bucket/foo',
            metadata=metadata,
            type=type_,
        )
        artifact_list = pipeline_spec_pb2.ArtifactList(
            artifacts=[runtime_artifact])

        actual = local_task.artifact_list_to_dsl_artifact(
            artifact_list,
            is_artifact_list=True,
        )
        expected = [
            dsl.Dataset(
                name='a',
                uri='gs://bucket/foo',
                metadata={'foo': 'bar'},
            )
        ]
        self.assertEqual(actual, expected)

    def test_multi_entry_list(self):
        metadata = struct_pb2.Struct()
        metadata.fields['foo'].string_value = 'bar'
        type_ = pipeline_spec_pb2.ArtifactTypeSchema(
            schema_title='system.Dataset',
            schema_version='0.0.1',
        )
        runtime_artifact1 = pipeline_spec_pb2.RuntimeArtifact(
            name='a',
            uri='gs://bucket/foo/a',
            metadata=metadata,
            type=type_,
        )
        runtime_artifact2 = pipeline_spec_pb2.RuntimeArtifact(
            name='b',
            uri='gs://bucket/foo/b',
            type=type_,
        )
        artifact_list = pipeline_spec_pb2.ArtifactList(
            artifacts=[runtime_artifact1, runtime_artifact2])

        actual = local_task.artifact_list_to_dsl_artifact(
            artifact_list,
            is_artifact_list=True,
        )
        expected = [
            dsl.Dataset(
                name='a',
                uri='gs://bucket/foo/a',
                metadata={'foo': 'bar'},
            ),
            dsl.Dataset(
                name='b',
                uri='gs://bucket/foo/b',
            )
        ]
        self.assertEqual(actual, expected)


class AddTypeToExecutorOutput(unittest.TestCase):

    def test(self):
        ...


class TestLocalTask(unittest.TestCase):

    def test_init_constructor(self):
        ...

    def test_from_messages(self):
        ...


if __name__ == '__main__':
    unittest.main()
