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
import os
import unittest
from unittest import mock

import docker
from kfp import dsl
from kfp import local
from kfp.dsl import Artifact
from kfp.dsl import Output
from kfp.local import docker_task_handler
from kfp.local import testing_utilities
import unittest
from unittest import mock
from kfp.local import docker_task_handler
from typing import List, Dict, Any

from kfp import dsl
from kfp import local
from kfp.dsl import pipeline_task


def run_docker_container_mock(
    client: 'docker.DockerClient',
    image: str,
    command: List[str],
    volumes: Dict[str, Any],
) -> int:
    """Run commands/args in a subprocess. Effectively the same as the SubprocessRunner, but allows more than just Container Components.
    
    This allows us to test the Container Component and placeholder resolution logic E2E."""
    from kfp.local import subprocess_task_handler
    return subprocess_task_handler.run_local_subprocess(command)


class DockerContainerClientMock(unittest.TestCase):
    """Mocks the client"""

    def setUp(self):
        super().setUp()
        self.docker_mock = mock.Mock()
        patcher = mock.patch('docker.from_env')
        self.mocked_docker_client = patcher.start().return_value

        mock_container = mock.Mock()
        self.mocked_docker_client.containers.run.return_value = mock_container
        # mock successful run
        mock_container.logs.return_value = [
            'fake'.encode('utf-8'),
            'container'.encode('utf-8'),
            'logs'.encode('utf-8'),
        ]
        mock_container.wait.return_value = {'StatusCode': 0}

    def teardown(self):
        super().tearDown()
        self.docker_mock.reset_mock()


class TestRunDockerContainer(DockerContainerClientMock):

    def test_no_volumes(self):
        docker_task_handler.run_docker_container(
            docker.from_env(),
            image='alpine',
            command=['echo', 'foo'],
            volumes={},
        )

        self.mocked_docker_client.containers.run.assert_called_once_with(
            image='alpine:latest',
            command=['echo', 'foo'],
            detach=True,
            stdout=True,
            stderr=True,
            volumes={},
        )

    def test_cwd_volume(self):
        current_test_dir = os.path.dirname(os.path.abspath(__file__))
        docker_task_handler.run_docker_container(
            client=docker.from_env(),
            image='alpine',
            command=['cat', '/localdir/docker_task_handler_test.py'],
            volumes={current_test_dir: {
                'bind': '/localdir',
                'mode': 'ro'
            }},
        )
        self.mocked_docker_client.containers.run.assert_called_once_with(
            image='alpine:latest',
            command=['cat', '/localdir/docker_task_handler_test.py'],
            detach=True,
            stdout=True,
            stderr=True,
            volumes={current_test_dir: {
                'bind': '/localdir',
                'mode': 'ro'
            }})


class TestDockerTaskHandler(DockerContainerClientMock):

    def test_get_volumes_to_mount(self):
        handler = docker_task_handler.DockerTaskHandler(
            image='alpine',
            full_command=['echo', 'foo'],
            pipeline_root=os.path.abspath('my_root'),
            runner=local.DockerRunner(),
        )
        volumes = handler.get_volumes_to_mount()
        self.assertEqual(
            volumes, {
                os.path.abspath('my_root'): {
                    'bind': os.path.abspath('my_root'),
                    'mode': 'rw'
                }
            })

    def test_run(self):
        handler = docker_task_handler.DockerTaskHandler(
            image='alpine',
            full_command=['echo', 'foo'],
            pipeline_root=os.path.abspath('my_root'),
            runner=local.DockerRunner(),
        )

        handler.run()
        self.mocked_docker_client.containers.run.assert_called_once_with(
            image='alpine:latest',
            command=['echo', 'foo'],
            detach=True,
            stdout=True,
            stderr=True,
            volumes={
                os.path.abspath('my_root'): {
                    'bind': os.path.abspath('my_root'),
                    'mode': 'rw'
                }
            },
        )

    def test_pipeline_root_relpath(self):
        with self.assertRaisesRegex(
                ValueError,
                r"'pipeline_root' should be an absolute path to correctly construct the volume mount specification\."
        ):
            docker_task_handler.DockerTaskHandler(
                image='alpine',
                full_command=['echo', 'foo'],
                pipeline_root='my_relpath',
                runner=local.DockerRunner(),
            ).run()


class TestAddLatestTagIfNotPresent(unittest.TestCase):

    def test_no_tag(self):
        actual = docker_task_handler.add_latest_tag_if_not_present(
            image='alpine')
        expected = 'alpine:latest'
        self.assertEqual(actual, expected)

    def test_latest_tag(self):
        actual = docker_task_handler.add_latest_tag_if_not_present(
            image='alpine:latest')
        expected = 'alpine:latest'
        self.assertEqual(actual, expected)

    def test_no_tag(self):
        actual = docker_task_handler.add_latest_tag_if_not_present(
            image='alpine:123')
        expected = 'alpine:123'
        self.assertEqual(actual, expected)


class TestPythonComponent(DockerContainerClientMock,
                          testing_utilities.LocalRunnerEnvironmentTestCase):

    def test_python(self):
        local.init(runner=local.DockerRunner())

        @dsl.component
        def artifact_maker(x: str, a: Output[Artifact]):
            with open(a.path, 'w') as f:
                f.write(x)

        try:
            artifact_maker(x='foo')
        # cannot get outputs if they aren't created due to mock
        except FileNotFoundError:
            pass

        run_mock = self.mocked_docker_client.containers.run
        run_mock.assert_called_once()
        kwargs = run_mock.call_args[1]
        self.assertEqual(
            kwargs['image'],
            'python:3.7',
        )
        self.assertTrue(
            any('def artifact_maker' in c for c in kwargs['command']))
        self.assertTrue(kwargs['detach'])
        self.assertTrue(kwargs['stdout'])
        self.assertTrue(kwargs['stderr'])
        root_vol_key = [
            key for key in kwargs['volumes'].keys() if 'local_outputs' in key
        ][0]
        self.assertEqual(kwargs['volumes'][root_vol_key]['bind'], root_vol_key)
        self.assertEqual(kwargs['volumes'][root_vol_key]['mode'], 'rw')

    def test_empty_container_component(self):
        local.init(runner=local.DockerRunner())

        @dsl.container_component
        def comp():
            return dsl.ContainerSpec(image='alpine')

        try:
            comp()
        # cannot get outputs if they aren't created due to mock
        except FileNotFoundError:
            pass

        run_mock = self.mocked_docker_client.containers.run
        run_mock.assert_called_once()
        kwargs = run_mock.call_args[1]
        self.assertEqual(
            kwargs['image'],
            'alpine:latest',
        )
        self.assertEqual(kwargs['command'], [])

    def test_container_component(self):
        local.init(runner=local.DockerRunner())

        @dsl.container_component
        def artifact_maker(x: str,):
            return dsl.ContainerSpec(
                image='alpine',
                command=['sh', '-c', f'echo prefix-{x}'],
            )

        try:
            artifact_maker(x='foo')
        # cannot get outputs if they aren't created due to mock
        except FileNotFoundError:
            pass

        run_mock = self.mocked_docker_client.containers.run
        run_mock.assert_called_once()
        kwargs = run_mock.call_args[1]
        self.assertEqual(
            kwargs['image'],
            'alpine:latest',
        )
        self.assertEqual(kwargs['command'], [
            'sh',
            '-c',
            'echo prefix-foo',
        ])
        self.assertTrue(kwargs['detach'])
        self.assertTrue(kwargs['stdout'])
        self.assertTrue(kwargs['stderr'])
        root_vol_key = [
            key for key in kwargs['volumes'].keys() if 'local_outputs' in key
        ][0]
        self.assertEqual(kwargs['volumes'][root_vol_key]['bind'], root_vol_key)
        self.assertEqual(kwargs['volumes'][root_vol_key]['mode'], 'rw')


class TestE2EContainerComponent(
        testing_utilities.LocalRunnerEnvironmentTestCase,):

    def setUp(self):
        super().setUp()
        self.docker_mock = mock.Mock()
        patcher = mock.patch('docker.from_env')
        self.mocked_docker_client = patcher.start().return_value

        self.run_docker_container_patch = mock.patch.object(
            docker_task_handler,
            'run_docker_container',
            side_effect=run_docker_container_mock,
        )
        self.mocked_run_docker_container = self.run_docker_container_patch.start(
        )

    def teardown(self):
        super().tearDown()
        self.docker_mock.reset_mock()
        self.mocked_docker_client.reset_mock()
        self.run_docker_container_patch.stop()

    def test_parameters(self):

        local.init(runner=local.DockerRunner())

        pipeline_task.TEMPORARILY_BLOCK_LOCAL_EXECUTION = False

        @dsl.container_component
        def comp(
                str_in: str,
                int_in: int,
                float_in: float,
                bool_in: bool,
                dict_in: dict,
                list_in: list,
                str_out: dsl.OutputPath(str),
                int_out: dsl.OutputPath(int),
                float_out: dsl.OutputPath(float),
                bool_out: dsl.OutputPath(bool),
                dict_out: dsl.OutputPath(dict),
                list_out: dsl.OutputPath(list),
        ):
            return dsl.ContainerSpec(
                image='alpine',
                command=[
                    'sh',
                    '-c',
                ],
                args=[
                    f'mkdir -p $(dirname {str_out}) && echo {str_in} > {str_out} &&'
                    f'mkdir -p $(dirname {int_out}) && echo "{int_in}" > {int_out} &&'
                    f'mkdir -p $(dirname {float_out}) && echo "{float_in}" > {float_out} &&'
                    f'mkdir -p $(dirname {bool_out}) && echo {bool_in} > {bool_out} && '
                    f'mkdir -p $(dirname {dict_out}) && echo {dict_in}  > {dict_out} && '
                    f'mkdir -p $(dirname {list_out}) && echo \'["a", "b", "c"]\' > {list_out}'
                ],
            )

        task = comp(
            str_in='foo',
            int_in=100,
            float_in=2.718,
            bool_in=False,
            dict_in={'x': 'y'},
            list_in=['a', 'b', 'c'])
        self.assertEqual(task.outputs['str_out'], 'foo')
        self.assertEqual(task.outputs['int_out'], 100)
        self.assertEqual(task.outputs['float_out'], 2.718)
        self.assertFalse(task.outputs['bool_out'])
        self.assertEqual(task.outputs['dict_out'], {'x': 'y'})
        self.assertEqual(task.outputs['list_out'], ['a', 'b', 'c'])


if __name__ == '__main__':
    unittest.main()
