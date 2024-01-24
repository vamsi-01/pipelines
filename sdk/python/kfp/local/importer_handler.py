# Copyright 2024 The Kubeflow Authors
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
"""Code for running a dsl.importer node locally."""
import functools
import logging
from typing import Any, Dict, Tuple

from google.protobuf import json_format
from kfp import dsl
from kfp.local import logging_utils
from kfp.local import placeholder_utils
from kfp.local import status
from kfp.pipeline_spec import pipeline_spec_pb2


def run_importer(
    pipeline_resource_name: str,
    component_name: str,
    component_spec: pipeline_spec_pb2.ComponentSpec,
    executor_spec: pipeline_spec_pb2.PipelineDeploymentConfig.ExecutorSpec,
    arguments: Dict[str, Any],
    pipeline_root: str,
) -> Tuple[Dict[str, dsl.Artifact], status.Status]:
    """Runs an importer component and returns a two-tuple of (outputs, status).

    Args:
        pipeline_resource_name: The root pipeline resource name.
        component_name: The name of the component.
        component_spec: The ComponentSpec of the importer.
        executor_spec: The ExecutorSpec of the importer.
        arguments: The arguments to the importer, as determined by the TaskInputsSpec for the importer.
        pipeline_root: The local pipeline root directory of the current pipeline.

    Returns:
        A two-tuple of the output dictionary ({"artifact": <the-artifact>}) and the status. The outputs dictionary will be empty when status is failure.
    """

    from kfp.local import executor_input_utils
    task_resource_name = executor_input_utils.get_local_task_resource_name(
        component_name)
    task_name_for_logs = logging_utils.format_task_name(task_resource_name)
    with logging_utils.local_logger_context():
        logging.info(f'Executing task {task_name_for_logs}')
    task_root = executor_input_utils.construct_local_task_root(
        pipeline_root=pipeline_root,
        pipeline_resource_name=pipeline_resource_name,
        task_resource_name=task_resource_name,
    )
    executor_input = executor_input_utils.construct_executor_input(
        component_spec=component_spec,
        arguments=arguments,
        task_root=task_root,
        block_input_artifact=True,
    )
    importer = executor_spec.importer
    value_or_runtime_param = importer.artifact_uri.WhichOneof('value')
    if value_or_runtime_param == 'constant':
        # TODO: warn if remote path
        # TODO: check (raise or warn?) if local path exists
        # TODO: else, permit it...
        uri = importer.artifact_uri.constant.string_value
    elif value_or_runtime_param == 'runtime_parameter':
        uri = executor_input.inputs.parameter_values['uri'].string_value
    else:
        raise ValueError(
            f'Got unknown value of artifact_uri: {value_or_runtime_param}')

    metadata = json_format.MessageToDict(importer.metadata)
    executor_input_dict = executor_input_utils.executor_input_to_dict(
        executor_input=executor_input,
        component_spec=component_spec,
    )
    metadata = resolve_metadata_placeholders(
        metadata,
        executor_input_dict=executor_input_dict,
        pipeline_resource_name=pipeline_resource_name,
        task_resource_name=task_resource_name,
        pipeline_root=pipeline_root,
        # TODO: handle
        pipeline_job_id='pipeline_job_id',
        # TODO: handle
        pipeline_task_id='pipeline_task_id',
    )
    outputs = {
        'artifact': dsl.Artifact(
            name='artifact',
            uri=uri,
            metadata=metadata,
        )
    }
    with logging_utils.local_logger_context():
        logging.info(
            f'Task {task_name_for_logs} finished with status {logging_utils.format_status(status.Status.SUCCESS)}'
        )
        output_string = [
            f'Task {task_name_for_logs} outputs:',
            *logging_utils.make_log_lines_for_outputs(outputs),
        ]
        logging.info('\n'.join(output_string))
        logging_utils.print_horizontal_line()

    return outputs, status.Status.SUCCESS


def resolve_metadata_placeholders(
    obj: Any,
    executor_input_dict: Dict[str, Any],
    pipeline_resource_name: str,
    task_resource_name: str,
    pipeline_root: str,
    pipeline_job_id: str,
    pipeline_task_id: str,
) -> Any:
    """Recursively resolves any placeholders in obj.
    
    Metadata objects are very unlikely to be sufficiently large to exceed max recursion depth of 1000 and a iterative implementation is much less readable.
    """
    inner_fn = functools.partial(
        resolve_metadata_placeholders,
        executor_input_dict=executor_input_dict,
        pipeline_resource_name=pipeline_resource_name,
        task_resource_name=task_resource_name,
        pipeline_root=pipeline_root,
        pipeline_job_id=pipeline_job_id,
        pipeline_task_id=pipeline_task_id,
    )
    if isinstance(obj, list):
        return [inner_fn(item) for item in obj]
    elif isinstance(obj, dict):
        return {inner_fn(key): inner_fn(value) for key, value in obj.items()}
    elif isinstance(obj, str):
        return placeholder_utils.resolve_individual_placeholder(
            element=obj,
            executor_input_dict=executor_input_dict,
            pipeline_resource_name=pipeline_resource_name,
            task_resource_name=task_resource_name,
            pipeline_root=pipeline_root,
            pipeline_job_id=pipeline_job_id,
            pipeline_task_id=pipeline_task_id,
        )
    else:
        return obj
