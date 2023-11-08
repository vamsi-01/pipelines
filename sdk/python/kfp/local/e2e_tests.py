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
"""E2E local execution tests.

Parameterized across both runner types.
"""
import functools
from typing import NamedTuple
import unittest

from absl.testing import parameterized
from kfp import dsl
from kfp import local
from kfp.dsl import Artifact
from kfp.dsl import Dataset
from kfp.dsl import Output
from kfp.local import subprocess_runner


@parameterized.parameters([
    (local.SubprocessRunner(use_venv=False),),
])
class TestLightweightPythonComponents(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # silence logging for these tests
        cls.original_run_local_subprocess = subprocess_runner.run_local_subprocess
        subprocess_runner.run_local_subprocess = functools.partial(
            subprocess_runner.run_local_subprocess,
            log_to_stdout=False,
        )

    @classmethod
    def tearDownClass(cls):
        subprocess_runner.run_local_subprocess = cls.original_run_local_subprocess

    def test_str_input(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def identity(x: str) -> str:
            return x

        actual = identity(x='hello').output
        expected = 'hello'
        # since == is overloaded, we need to be careful that this assertion
        # doesn't resolve to a ConditionOperation which is truthy, giving a
        # false negative
        self.assertIsInstance(actual, str)
        self.assertEqual(actual, expected)

    # def test_int_input(self, runner):
    #     local_runner.init(runner=runner, cleanup=True)

    #     @dsl.component
    #     def identity(x: int) -> int:
    #         return x
    # TODO: casting needed? where is the diff between local and cloud?
    #     actual = identity(x=1).output
    #     expected = 1
    #     self.assertIsInstance(actual, int)
    #     self.assertEqual(actual, expected)

    def test_float_input(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def identity(x: float) -> float:
            return x

        actual = identity(x=1.0).output
        expected = 1.0
        self.assertIsInstance(actual, float)
        self.assertEqual(actual, expected)

    def test_bool_input(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def identity(x: bool) -> bool:
            return x

        actual = identity(x=True).output
        self.assertIsInstance(actual, bool)
        self.assertTrue(actual)

    def test_list_input(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def identity(x: list) -> list:
            return x

        actual = identity(x=['a', 'b']).output
        expected = ['a', 'b']
        self.assertIsInstance(actual, list)
        self.assertEqual(actual, expected)

    def test_dict_input(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def identity(x: dict) -> dict:
            return x

        actual = identity(x={'a': 'b'}).output
        expected = {'a': 'b'}
        self.assertIsInstance(actual, dict)
        self.assertEqual(actual, expected)

    def test_multiple_parameter_outputs(self, runner):
        local.init(runner=runner, cleanup=True)
        from typing import NamedTuple

        @dsl.component
        def return_twice(x: str) -> NamedTuple('Outputs', x=str, y=str):
            Outputs = NamedTuple('Output', x=str, y=str)
            return Outputs(x=x, y=x)

        local_task = return_twice(x='foo')
        self.assertIsInstance(local_task.outputs['x'], str)
        self.assertEqual(local_task.outputs['x'], 'foo')
        self.assertIsInstance(local_task.outputs['y'], str)
        self.assertEqual(local_task.outputs['y'], 'foo')

    def test_single_output_not_available(self, runner):
        local.init(runner=runner, cleanup=True)
        from typing import NamedTuple

        @dsl.component
        def return_twice(x: str) -> NamedTuple('Outputs', x=str, y=str):
            Outputs = NamedTuple('Output', x=str, y=str)
            return Outputs(x=x, y=x)

        local_task = return_twice(x='foo')
        with self.assertRaisesRegex(
                AttributeError,
                r'The task has multiple outputs\. Please reference the output by its name\.'
        ):
            local_task.output

    def test_single_artifact_output_traditional(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def artifact_maker(x: str, a: Output[Artifact]):
            with open(a.path, 'w') as f:
                f.write(x)

            a.metadata['foo'] = 'bar'

        actual = artifact_maker(x='hello').output
        self.assertIsInstance(actual, Artifact)
        self.assertEqual(actual.name, 'a')
        self.assertTrue(actual.uri.endswith('/a'))
        with open(actual.path) as f:
            contents = f.read()
        self.assertEqual(contents, 'hello')
        self.assertEqual(actual.metadata, {'foo': 'bar'})

    def test_single_artifact_output_pythonic(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def artifact_maker(x: str) -> Artifact:
            artifact = Artifact(
                name='a', uri=dsl.get_uri('a'), metadata={'foo': 'bar'})
            with open(artifact.path, 'w') as f:
                f.write(x)

            return artifact

        actual = artifact_maker(x='hello').output
        self.assertIsInstance(actual, Artifact)
        self.assertEqual(actual.name, 'a')
        self.assertTrue(actual.uri.endswith('/a'))
        with open(actual.path) as f:
            contents = f.read()
        self.assertEqual(contents, 'hello')
        self.assertEqual(actual.metadata, {'foo': 'bar'})

    def test_multiple_artifact_outputs_traditional(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def double_artifact_maker(
            x: str,
            y: str,
            a: Output[Artifact],
            b: Output[Dataset],
        ):
            with open(a.path, 'w') as f:
                f.write(x)

            with open(b.path, 'w') as f:
                f.write(y)

            a.metadata['foo'] = 'bar'
            b.metadata['baz'] = 'bat'

        local_task = double_artifact_maker(x='hello', y='goodbye')

        actual_a = local_task.outputs['a']
        actual_b = local_task.outputs['b']

        self.assertIsInstance(actual_a, Artifact)
        self.assertEqual(actual_a.name, 'a')
        self.assertTrue(actual_a.uri.endswith('/a'))
        with open(actual_a.path) as f:
            contents = f.read()
        self.assertEqual(contents, 'hello')
        self.assertEqual(actual_a.metadata, {'foo': 'bar'})

        self.assertIsInstance(actual_b, Dataset)
        self.assertEqual(actual_b.name, 'b')
        self.assertTrue(actual_b.uri.endswith('/b'))
        with open(actual_b.path) as f:
            contents = f.read()
        self.assertEqual(contents, 'goodbye')
        self.assertEqual(actual_b.metadata, {'baz': 'bat'})

    def test_multiple_artifact_outputs_pythonic(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def double_artifact_maker(
            x: str,
            y: str,
        ) -> NamedTuple(
                'Outputs', a=Artifact, b=Dataset):
            a = Artifact(
                name='a', uri=dsl.get_uri('a'), metadata={'foo': 'bar'})
            b = Dataset(name='b', uri=dsl.get_uri('b'), metadata={'baz': 'bat'})

            with open(a.path, 'w') as f:
                f.write(x)

            with open(b.path, 'w') as f:
                f.write(y)

            Outputs = NamedTuple('Outputs', a=Artifact, b=Dataset)
            return Outputs(a=a, b=b)

        local_task = double_artifact_maker(x='hello', y='goodbye')

        actual_a = local_task.outputs['a']
        actual_b = local_task.outputs['b']

        self.assertIsInstance(actual_a, Artifact)
        self.assertEqual(actual_a.name, 'a')
        self.assertTrue(actual_a.uri.endswith('/a'))
        with open(actual_a.path) as f:
            contents = f.read()
        self.assertEqual(contents, 'hello')
        self.assertEqual(actual_a.metadata, {'foo': 'bar'})

        self.assertIsInstance(actual_b, Dataset)
        self.assertEqual(actual_b.name, 'b')
        self.assertTrue(actual_b.uri.endswith('/b'))
        with open(actual_b.path) as f:
            contents = f.read()
        self.assertEqual(contents, 'goodbye')
        self.assertEqual(actual_b.metadata, {'baz': 'bat'})

    def test_str_input_uses_default(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def identity(x: str = 'hi') -> str:
            return x

        actual = identity().output
        expected = 'hi'
        # since == is overloaded, we need to be careful that this assertion
        # doesn't resolve to a ConditionOperation which is truthy, giving a
        # false negative
        self.assertIsInstance(actual, str)
        self.assertEqual(actual, expected)

        # def test_int_input_uses_default(self, runner):
        local.init(runner=runner, cleanup=True)

    #     @dsl.component
    #     def identity(x: int = 1) -> int:
    #         return 1

    #     actual = identity().output
    #     expected = 1
    #     # since == is overloaded, we need to be careful that this assertion
    #     # doesn't resolve to a ConditionOperation which is truthy, giving a
    #     # false negative
    #     self.assertIsInstance(actual, int)
    #     self.assertEqual(actual, expected)


@parameterized.parameters([
    (local.SubprocessRunner(use_venv=False),),
])
class TestInvalidArguments(unittest.TestCase):

    def setUp(self):
        local.init(cleanup=True)

    @classmethod
    def setUpClass(cls):
        # silence logging for these tests
        cls.original_run_local_subprocess = subprocess_runner.run_local_subprocess
        subprocess_runner.run_local_subprocess = functools.partial(
            subprocess_runner.run_local_subprocess,
            log_to_stdout=False,
        )

    @classmethod
    def tearDownClass(cls):
        subprocess_runner.run_local_subprocess = cls.original_run_local_subprocess

    def test_no_argument_no_default(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def identity(x: str) -> str:
            return x

        with self.assertRaisesRegex(
                RuntimeError,
                r"TypeError: identity\(\) missing 1 required positional argument: 'x'"
        ):
            task = identity()

    # # TODO: maybe throw invalid type error
    # def test_default_wrong_type(self, runner):
        local.init(runner=runner, cleanup=True)

    #     @dsl.component
    #     def identity(x: str) -> str:
    #         return x

    #     task = identity(x=1)

    def test_input_artifact_provided(self, runner):
        local.init(runner=runner, cleanup=True)

        @dsl.component
        def identity(a: Artifact) -> Artifact:
            return a

        with self.assertRaisesRegex(
                ValueError,
                'Input artifacts are not yet supported for local execution.'):
            identity(a=Artifact(name='a', uri='gs://bucket/foo'))
