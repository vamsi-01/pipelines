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
"""Implementation of the subprocess runner."""

from typing import List

from docker.models import containers
from kfp.local import local_task
from kfp.pipeline_spec import pipeline_spec_pb2
import requests
import urllib3


class ContainerRunnerImpl:

    def __init__(
        self,
        full_command: List[str],
        image: str,
        pipeline_root: str,
        executor_input: pipeline_spec_pb2.ExecutorInput,
        component_spec: pipeline_spec_pb2.ComponentSpec,
    ) -> None:
        self.full_command = full_command
        self.image = image
        self.pipeline_root = pipeline_root
        self.executor_input = executor_input
        self.component_spec = component_spec

    def run(self) -> local_task.LocalTask:
        run_component_in_container(self.full_command, self.image)
        return local_task.LocalTask._from_messages(self.executor_input,
                                                   self.component_spec)


def run_component_in_container(
    full_command: List[str],
    image: str,
    pipeline_root: str,
) -> None:
    try:
        import docker
    # TODO: put in init
    except Exception as e:
        raise e
    try:
        client = docker.from_env()
    # TODO: put in init
    except (
            requests.exceptions.ConnectionError,
            FileNotFoundError,
            urllib3.exceptions.ProtocolError,
            docker.errors.DockerException,
    ) as e:
        raise docker.errors.DockerException(
            'Docker is not running. Please start Docker and try again.') from e

    try:
        # TODO: what to do about this?
        from kfp.dsl.executor import makedirs_recursively
        makedirs_recursively(pipeline_root)
        container = client.containers.run(
            image,
            command=full_command,
            environment={'LOCAL_SESSION': 'true'},
            detach=True,
            volumes={
                pipeline_root: {
                    'bind': pipeline_root,
                    'mode': 'rw',
                },
            },
        )
        print(image)
        print(' '.join(full_command))
        print(f'Container {container.id} is running.')

        # # Start a new thread for streaming logs
        # log_thread = threading.Thread(target=stream_container_logs, args=(container,))
        # log_thread.start()

        # # You can perform other operations here while the logs are being streamed by the separate thread
        # # ...

        # # Optionally wait for the log thread to finish (for example, if the main program is doing nothing else)
        # log_thread.join()

    except docker.errors.ContainerError as e:
        print(f'Container run failed: {e}')
    except docker.errors.ImageNotFound as e:
        print(f'Image not found: {e}')
    except docker.errors.APIError as e:
        print(f'Server returned an error: {e}')

    for log_line in container.logs(stream=True):
        print(log_line.decode().strip())


def stream_container_logs(container: containers.Container) -> None:
    """Stream logs from the container."""
    for log in container.logs(stream=True):
        print(log.decode().strip())
