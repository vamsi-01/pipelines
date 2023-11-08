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

import subprocess
import sys
from typing import List

from kfp.local import local_task
from kfp.pipeline_spec import pipeline_spec_pb2


class SubprocessRunnerImpl:

    def __init__(
        self,
        full_command: List[str],
        pipeline_root: str,
        executor_input: pipeline_spec_pb2.ExecutorInput,
        component_spec: pipeline_spec_pb2.ComponentSpec,
    ) -> None:
        self.full_command = full_command
        self.pipeline_root = pipeline_root
        self.executor_input = executor_input
        self.component_spec = component_spec

    def run(self) -> local_task.LocalTask:
        full_command = use_current_python_executable(self.full_command)
        run_local_subprocess(full_command=full_command)
        return local_task.LocalTask._from_messages(self.executor_input,
                                                   self.component_spec)


def run_local_subprocess(
    full_command: List[str],
    log_to_stdout: bool = True,
) -> None:
    result = subprocess.run(
        full_command,
        capture_output=True,
        text=True,
        env={'LOCAL_SESSION': 'true'},
    )

    if result.returncode != 0:
        raise RuntimeError(result.stderr)
    if log_to_stdout:
        # print instead of log since the executor_main.py file's outputs are
        # already logged
        # we don't want multiple layers of logging prefixes
        print(result.stdout)


def use_current_python_executable(full_command: List[str]) -> List[str]:
    return [el.replace('python3 ', f'{sys.executable} ') for el in full_command]
