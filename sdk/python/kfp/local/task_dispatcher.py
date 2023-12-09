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
"""Code for dispatching a local task execution."""
import json
import logging
from typing import Any, Dict

from kfp import local
from kfp.local import config
from kfp.local import executor_input_utils
from kfp.local import placeholder_utils
from kfp import dsl
from kfp import local
from kfp.local import config
from kfp.local import executor_input_utils
from kfp.local import executor_output_utils
from kfp.local import logging_utils
from kfp.local import placeholder_utils
from kfp.local import status
from kfp.local import subprocess_task_handler
from kfp.local import task_handler_interface
from kfp.pipeline_spec import pipeline_spec_pb2


def run_single_component(
    pipeline_spec: pipeline_spec_pb2.PipelineSpec,
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    """Runs a single component from its compiled PipelineSpec.

    Args:
        pipeline_spec: The PipelineSpec of the component to run.
        arguments: The runtime arguments.

    Returns:
        A LocalTask instance.
    """
    if config.LocalExecutionConfig.instance is None:
        raise RuntimeError(
            f"Local environment not initialized. Please run '{local.__name__}.{local.init.__name__}()' before executing tasks locally."
        )

    return _run_single_component_implementation(
        pipeline_spec=pipeline_spec,
        arguments=arguments,
        pipeline_root=config.LocalExecutionConfig.instance.pipeline_root,
        runner=config.LocalExecutionConfig.instance.runner,
    )


def _run_single_component_implementation(
    pipeline_spec: pipeline_spec_pb2.PipelineSpec,
    arguments: Dict[str, Any],
    pipeline_root: str,
    runner: config.LocalRunnerType,
) -> Dict[str, Any]:
    """The implementation of a single component runner."""

    component_name, component_spec = list(pipeline_spec.components.items())[0]

    pipeline_resource_name = executor_input_utils.get_local_pipeline_resource_name(
        pipeline_spec.pipeline_info.name)
    task_resource_name = executor_input_utils.get_local_task_resource_name(
        component_name)
    task_root = executor_input_utils.construct_local_task_root(
        pipeline_root=pipeline_root,
        pipeline_resource_name=pipeline_resource_name,
        task_resource_name=task_resource_name,
    )
    executor_input = executor_input_utils.construct_executor_input(
        component_spec=component_spec,
        arguments=arguments,
        task_root=task_root,
    )

    executor_spec = pipeline_spec.deployment_spec['executors'][
        component_spec.executor_label]

    container = executor_spec['container']
    full_command = list(container['command']) + list(container['args'])

    # image + full_command are "inputs" to local execution
    image = container['image']
    # TODO: handler container component placeholders when
    # ContainerRunner is implemented
    full_command = placeholder_utils.replace_placeholders(
        full_command=full_command,
        executor_input=executor_input,
        pipeline_resource_name=pipeline_resource_name,
        task_resource_name=task_resource_name,
        pipeline_root=pipeline_root,
    )

    runner_type = type(runner)
    task_handler_map: Dict[
        local.LocalRunnerType, task_handler_interface.TaskHandlerInterface] = {
            local.SubprocessRunner:
                subprocess_task_handler.SubprocessTaskHandler,
        }
    if runner_type not in task_handler_map:
        raise ValueError(f'Got unknown argument for runner: {runner}.')
    TaskHandler = task_handler_map[runner_type]
    task_name_for_logs = logging_utils.color_text(f'{task_resource_name!r}',
                                                  logging_utils.Colors.CYAN)

    with logging_utils.local_logger_context():
        logging.info(f'Executing task {task_name_for_logs}')
        task_handler = TaskHandler(
            image=image,
            full_command=full_command,
            pipeline_root=pipeline_root,
            runner=runner,
        )

        # trailing newline helps visually separate subprocess logs
        logging.info(f'Streaming logs for task {task_name_for_logs}:\n')

        with logging_utils.indented_print():
            task_status = task_handler.run()

        if task_status == status.Status.SUCCESS:
            logging.info(
                f'Task {task_name_for_logs} finished with status {logging_utils.color_text(task_status.value, logging_utils.Colors.GREEN)}'
            )

            outputs = executor_output_utils.get_outputs_from_messages(
                executor_input=executor_input,
                component_spec=component_spec,
            )
            output_string = f'Task {task_name_for_logs} outputs:\n'
            for key, value in outputs.items():
                if isinstance(value, dsl.Artifact):
                    value = logging_utils.render_artifact(value)
                else:
                    value = json.dumps(value)
                output_string += f'{" " * 4}{key}: {value}\n'
            logging.info(output_string)

        elif task_status == status.Status.FAILURE:
            outputs = {}
            logging.error(
                f'Task {task_name_for_logs} finished with status {logging_utils.color_text(task_status.value, logging_utils.Colors.RED)}'
            )

        else:
            # for developers; user should never hit this
            raise ValueError(f'Got unknown status: {task_status}')

        return outputs
