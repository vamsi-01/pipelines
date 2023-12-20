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


@dsl.container_component
def comp(o: dsl.OutputPath(str)):
    return dsl.ContainerSpec(
        image='alpine',
        command=[
            'sh',
            '-c',
        ],
        args=[
            f'mkdir -p $(dirname {o}) && echo foo > {o}',
        ],
    )


class TestDockerContainerFunction(unittest.TestCase):

    def setUp(self):
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

    def test_run_docker_container(self):

        local.init(runner=local.DockerRunner())

        pipeline_task.TEMPORARILY_BLOCK_LOCAL_EXECUTION = False

        task = comp()
        self.assertEqual(task.output, 'foo')
