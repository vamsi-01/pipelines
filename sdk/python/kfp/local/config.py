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
"""Objects for configuring local execution."""
import abc
import dataclasses

import requests
import urllib3


class LocalRunnerType(abc.ABC):
    pass

    @abc.abstractmethod
    def validate(self) -> None:
        raise NotImplementedError


@dataclasses.dataclass
class SubprocessRunner(LocalRunnerType):
    use_venv: bool

    def validate(self):
        pass


@dataclasses.dataclass
class ContainerRunner(LocalRunnerType):

    def validate_docker_py_lib_is_installed(self) -> None:
        try:
            import docker
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                'The Docker SDK for Python is not installed. Please run `pip install docker` and try again.'
            ) from e

    def validate_docker_is_running(self) -> None:
        import docker

        try:
            docker.from_env()
        except (
                requests.exceptions.ConnectionError,
                FileNotFoundError,
                urllib3.exceptions.ProtocolError,
                docker.errors.DockerException,
        ) as e:
            raise docker.errors.DockerException(
                'Docker is not running. Please install Docker (https://docs.docker.com/engine/install/) if it is not already installed, start the Docker daemon, and try again.'
            ) from e

    def validate(self):
        self.validate_docker_py_lib_is_installed()
        self.validate_docker_is_running()


class LocalRunnerConfig:
    instance = None

    def __new__(
        cls,
        pipeline_root: str,
        runner: LocalRunnerType,
        cleanup: bool,
    ) -> 'LocalRunnerConfig':
        # singleton pattern
        cls.instance = super(LocalRunnerConfig, cls).__new__(cls)
        return cls.instance

    def __init__(
        self,
        pipeline_root: str,
        runner: LocalRunnerType,
        cleanup: bool,
    ) -> None:
        self.pipeline_root = pipeline_root
        self.runner = runner
        self.runner.validate()
        self.cleanup = cleanup


def init(
    pipeline_root: str = './local_outputs',
    runner: LocalRunnerType = SubprocessRunner(use_venv=True),
    cleanup: bool = False,
) -> None:
    # singleton
    LocalRunnerConfig(
        pipeline_root=pipeline_root,
        runner=runner,
        cleanup=cleanup,
    )
