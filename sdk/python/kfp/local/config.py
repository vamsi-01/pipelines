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
import os


class LocalRunnerType(abc.ABC):
    """The ABC for user-facing Runner configurations.

    Subclasses should be a dataclass.

    They should implement a .validate() method.
    """

    @abc.abstractmethod
    def validate(self) -> None:
        """Validates that the configuration arguments provided by the user are
        valid."""
        raise NotImplementedError


@dataclasses.dataclass
class SubprocessRunner(LocalRunnerType):
    """Runner that indicates to run tasks in a subprocess.

    Args:
        use_venv: Whether to use a virtual environment for any dependencies or install dependencies in the current environment. Setting this value to true is recommended.
    """
    use_venv: bool = True

    def validate(self) -> None:
        is_windows = os.name == 'nt'
        if is_windows and self.use_venv:
            raise ValueError(
                "This 'use_venv=True' is not supported on Windows.")


class LocalExecutionConfig:
    instance = None

    def __new__(
        cls,
        pipeline_root: str,
        runner: LocalRunnerType,
        cleanup: bool,
    ) -> 'LocalExecutionConfig':
        # singleton pattern
        cls.instance = super(LocalExecutionConfig, cls).__new__(cls)
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
    """Initializes a local execution session.

    Once called, components can be invoked locally outside of a pipeline definition.

    Args:
        pipeline_root: Destination for task outputs.
        runner: The runner to use. Currently only SubprocessRunner is supported.
        cleanup: Whether to ensure outputs are cleaned up after execution. If True, the task will be run in a temporary directory, rather than pipeline_root.
    """
    # updates a global config
    LocalExecutionConfig(
        pipeline_root=pipeline_root,
        runner=runner,
        cleanup=cleanup,
    )
