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
"""The class for a task created as a result local execution of a component."""

import inspect
from typing import Any, Dict, List, Optional

from kfp.dsl import pipeline_task_base


class LocalTask(pipeline_task_base.PipelineTaskBase):
    """A pipeline task which was executed locally.

    This is the result of a locally invoked component.
    """

    def __init__(
        self,
        outputs: Dict[str, Any],
        arguments: Dict[str, Any],
        task_name: str,
    ) -> None:
        """Constructs a LocalTask.

        Args:
            outputs: The dictionary of task outputs.
        """
        self._outputs = outputs
        self._arguments = arguments
        self._task_name = task_name

    @property
    def outputs(self) -> Dict[str, Any]:
        return self._outputs

    @property
    def output(self) -> Any:
        if len(self._outputs) != 1:
            raise AttributeError(
                'The task has multiple outputs. Please reference the output by its name.'
            )
        return list(self._outputs.values())[0]

    @property
    def platform_spec(self) -> Any:
        raise NotImplementedError(
            'Platform-specific features are not supported for local execution.')

    @property
    def name(self) -> str:
        return self._task_name

    @property
    def inputs(self) -> Dict[str, Any]:
        return self._arguments

    @property
    def dependent_tasks(self) -> List[pipeline_task_base.PipelineTaskBase]:
        raise NotImplementedError(
            'Task has no dependent tasks since it is executed outside of a pipeline.'
        )

    def set_caching_options(self, enable_caching: bool) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def set_cpu_request(self, cpu: str) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def set_cpu_limit(self, cpu: str) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def set_accelerator_limit(self, limit: int) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def set_gpu_limit(self, gpu: str) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def set_memory_request(self, memory: str) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def set_memory_limit(self, memory: str) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def set_retry(self,
                  num_retries: int,
                  backoff_duration: Optional[str] = None,
                  backoff_factor: Optional[float] = None,
                  backoff_max_duration: Optional[str] = None) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def add_node_selector_constraint(self, accelerator: str) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def set_display_name(self, name: str) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def set_env_variable(self, name: str, value: str) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def after(self, *tasks) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def ignore_upstream_failure(self) -> 'LocalTask':
        self._raise_task_configuration_not_implemented()

    def _raise_task_configuration_not_implemented(self) -> None:
        stack = inspect.stack()
        caller_name = stack[1].function
        raise NotImplementedError(
            f"Task configuration methods are not supported for local execution. Got call to '.{caller_name}()'."
        )
