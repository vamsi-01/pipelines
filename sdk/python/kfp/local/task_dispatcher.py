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
"""Code for executing DSL code locally."""
import json
import logging
import os
import tempfile
from typing import Any, Dict

from google.protobuf import json_format
from kfp import dsl
from kfp import local
from kfp.dsl import structures
from kfp.dsl.types import type_utils
from kfp.local import config
from kfp.local import executor_input_utils
from kfp.local import executor_output_utils
from kfp.local import local_task
from kfp.local import logging_utils
from kfp.local import placeholder_utils
from kfp.local import status
from kfp.local import subprocess_task_handler
from kfp.local import task_handler_interface
from kfp.pipeline_spec import pipeline_spec_pb2

# [] TODO: test in colab
# [] TODO: probably need a raise_on_error argument in init
# [] TODO: dsl.OutputPath special handling


def run_single_component(
    pipeline_spec: pipeline_spec_pb2.PipelineSpec,
    arguments: Dict[str, Any],
) -> local_task.LocalTask:
    """Runs a single component from its compiled PipelineSpec. This function
    should be used by other modules.

    All global state specified by the LocalExecutionConfig should be
    obtained here. Downstream calls should not read this state.
    """
    if config.LocalExecutionConfig.instance is None:
        raise RuntimeError(
            f'You must initiatize the local execution session using {local.__name__}.{local.init.__name__}().'
        )

    if config.LocalExecutionConfig.instance.cleanup:
        with tempfile.TemporaryDirectory() as tmpdir:
            temp_pipeline_root = os.path.join(tmpdir, 'temp_root')
            return _run_single_component_implementation(
                pipeline_spec=pipeline_spec,
                arguments=arguments,
                pipeline_root=temp_pipeline_root,
                runner=config.LocalExecutionConfig.instance.runner,
            )
    else:
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
) -> local_task.LocalTask:
    """The implementation of a single component runner."""

    if len(pipeline_spec.components) != 1:
        raise NotImplementedError(
            'Local pipeline execution is not currently supported.')

    component_name, component_spec = list(pipeline_spec.components.items())[0]

    if component_spec.input_definitions != pipeline_spec.root.input_definitions or component_spec.output_definitions != pipeline_spec.root.output_definitions:
        raise NotImplementedError(
            'Local pipeline execution is not currently supported.')
    # include the input artifact constant check above the validate_arguments check
    # for a better error message
    # we also perform this check further downstream in the executor input
    # construction utilities, since that's where the logic needs to be
    # implemented as well
    validate_no_input_artifact_constants(component_spec=component_spec)
    validate_arguments(
        arguments=arguments,
        component_spec=component_spec,
        component_name=component_name,
    )

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
    image = container['image']
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
            # user should never hit this
            # exists for future developers
            raise ValueError(f'Got unknown status: {task_status}')

        return local_task.LocalTask(
            outputs,
            arguments=arguments,
            task_name=task_resource_name,
        )


def validate_arguments(
    arguments: Dict[str, Any],
    component_spec: pipeline_spec_pb2.ComponentSpec,
    component_name: str,
) -> None:
    """Validates arguments provided for the execution of component_spec."""

    input_specs = {}
    for artifact_name, artifact_spec in component_spec.input_definitions.artifacts.items(
    ):
        input_specs[
            artifact_name] = structures.InputSpec.from_ir_component_inputs_dict(
                json_format.MessageToDict(artifact_spec))

    for parameter_name, parameter_spec in component_spec.input_definitions.parameters.items(
    ):
        input_specs[
            parameter_name] = structures.InputSpec.from_ir_component_inputs_dict(
                json_format.MessageToDict(parameter_spec))

    for input_name, argument_value in arguments.items():
        if input_name in input_specs:
            type_utils.verify_type_compatibility(
                given_value=argument_value,
                expected_spec=input_specs[input_name],
                error_message_prefix=(
                    f'Incompatible argument passed to the input '
                    f'{input_name!r} of component {component_name!r}: '),
            )
        else:
            raise ValueError(
                f'Component {component_name!r} got an unexpected input:'
                f' {input_name!r}.')


def validate_no_input_artifact_constants(
        component_spec: pipeline_spec_pb2.ComponentSpec) -> None:
    """Validates that the ComponentSpec doesn't accept any input artifact
    constants."""
    input_artifact_keys = list(
        component_spec.input_definitions.artifacts.keys())
    if input_artifact_keys:
        raise ValueError(
            'Input artifacts are not yet supported for local execution.')
