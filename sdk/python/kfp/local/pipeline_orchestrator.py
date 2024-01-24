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
"""Code for locally executing a compiled pipeline."""
import logging
from typing import Any, Dict

from kfp.local import config
from kfp.local import graph_utils
from kfp.local import importer_handler
from kfp.local import io
from kfp.local import logging_utils
from kfp.local import placeholder_utils
from kfp.local import status
from kfp.local import utils
from kfp.pipeline_spec import pipeline_spec_pb2


def run_local_pipeline(
    pipeline_spec: pipeline_spec_pb2.PipelineSpec,
    arguments: Dict[str, Any],
) -> Dict[str, Any]:
    """kfp.local's public entrypoint for running a local pipeline.

    Args:
        pipeline_spec: PipelineSpec of the pipeline to run.
        arguments: User-provided arguments.

    Returns:
        The pipeline outputs.
    """

    config.LocalExecutionConfig.validate()
    raise_on_error = config.LocalExecutionConfig.instance.raise_on_error
    return _run_local_pipeline_implementation(
        pipeline_spec=pipeline_spec,
        arguments=arguments,
        raise_on_error=raise_on_error,
    )


def _run_local_pipeline_implementation(
    pipeline_spec: pipeline_spec_pb2.PipelineSpec,
    arguments: Dict[str, Any],
    raise_on_error: bool,
) -> Dict[str, Any]:
    """Implementation of run local pipeline.

    Args:
        pipeline_spec: PipelineSpec of the pipeline to run.
        arguments: User-provided arguments.
        raise_on_error: Whether to raise if an inner task throws an error.

    Returns:
        The pipeline outputs.
    """
    pipeline_name = pipeline_spec.pipeline_info.name
    # create pipeline resource name for the root pipeline
    from kfp.local import executor_input_utils
    pipeline_resource_name = executor_input_utils.get_local_pipeline_resource_name(
        pipeline_name)
    pipeline_name_with_color = logging_utils.format_pipeline_name(
        pipeline_spec.pipeline_info.name)

    with logging_utils.local_logger_context():
        logging.info(f'Running pipeline: {pipeline_name_with_color}')
        logging_utils.print_horizontal_line()

    executors = pipeline_spec.deployment_spec['executors']
    executors = {
        name: utils.struct_to_executor_spec(executor)
        for name, executor in executors.items()
    }

    # convert to dict for consistency with executors data structure in run_dag
    components = dict(pipeline_spec.components.items())
    io_store = io.IOStore()
    task_status = run_dag(
        pipeline_name=pipeline_name,
        pipeline_resource_name=pipeline_resource_name,
        dag_component_spec=pipeline_spec.root,
        executors=executors,
        components=components,
        dag_arguments=arguments,
        io_store=io_store,
        raise_on_error=raise_on_error,
    )
    if task_status == status.Status.SUCCESS:
        output_params = get_dag_output_parameters(
            dag_outputs_spec=pipeline_spec.root.dag.outputs,
            io_store=io_store,
        )
        output_artifacts = get_dag_output_artifacts(
            dag_outputs_spec=pipeline_spec.root.dag.outputs,
            io_store=io_store,
        )
        return {**output_params, **output_artifacts}
    elif task_status == status.Status.FAILURE:
        return {}
    else:
        raise ValueError(f'Got unknown task status {task_status.name}')


def run_dag(
    pipeline_name: str,
    pipeline_resource_name: str,
    dag_component_spec: pipeline_spec_pb2.ComponentSpec,
    executors: Dict[str,
                    pipeline_spec_pb2.PipelineDeploymentConfig.ExecutorSpec],
    components: Dict[str, pipeline_spec_pb2.ComponentSpec],
    dag_arguments: Dict[str, Any],
    io_store: io.IOStore,
    raise_on_error: bool,
) -> status.Status:
    """Runs a DAGSpec.

    Args:
        pipeline_name: The human-readable pipeline name.
        pipeline_resource_name: The root pipeline resource name.
        dag_component_spec: The ComponentSpec which defines the DAG to execute.
        executors: The ExecutorSpec's from the root pipeline containing the DAG.
        components: The ComponentSpec's corresponding to the DAG.
        dag_arguments: The arguments to the DAG's outer ComponentSpec.
        io_store: The IOStore instance corresponding to this DAG.
        arguments: Whether to raise if an inner task throws an error.

    Returns:
        The Status of the DAG run.
    """
    # prepare IOStore for DAG
    dag_arguments_with_defaults = bind_defaults_to_dag_arguments(
        dag_arguments=dag_arguments,
        dag_inputs_spec=dag_component_spec.input_definitions,
    )
    for k, v in dag_arguments_with_defaults.items():
        io_store.put_parent_input(k, v)

    # execute tasks in order
    dag_spec = dag_component_spec.dag
    sorted_tasks = graph_utils.topological_sort_tasks(dag_spec.tasks)
    while sorted_tasks:
        task_name = sorted_tasks.pop()
        component_name = dag_spec.tasks[task_name].component_ref.name
        component_spec = components[component_name]
        implementation = component_spec.WhichOneof('implementation')
        # TODO: support pipeline-in-pipeline + control flow features
        if implementation == 'dag':
            raise ValueError(
                'Control flow features and pipelines in pipelines are are not yet supported in local pipeline execution.'
            )
        elif implementation != 'executor_label':
            raise ValueError(
                f'Got unknown component implementation: {implementation}')

        executor_key = component_spec.executor_label
        executor_spec = executors[executor_key]

        task_arguments = make_task_arguments(
            task_inputs_spec=dag_spec.tasks[task_name],
            io_store=io_store,
        )

        if executor_spec.WhichOneof('spec') == 'importer':
            outputs, task_status = importer_handler.run_importer(
                pipeline_resource_name=pipeline_resource_name,
                component_name=component_name,
                component_spec=component_spec,
                executor_spec=executor_spec,
                arguments=task_arguments,
                pipeline_root=config.LocalExecutionConfig.instance
                .pipeline_root,
            )
        elif executor_spec.WhichOneof('spec') == 'container':
            from kfp.local import task_dispatcher
            outputs, task_status = task_dispatcher._run_single_task_implementation(
                pipeline_resource_name=pipeline_resource_name,
                component_name=component_name,
                component_spec=component_spec,
                executor_spec=executor_spec,
                arguments=task_arguments,
                pipeline_root=config.LocalExecutionConfig.instance
                .pipeline_root,
                runner=config.LocalExecutionConfig.instance.runner,
                # let the outer pipeline raise the error
                raise_on_error=False,
                block_input_artifact=False,
                # provide the same unique job id for each tasks for
                # placeholder resolution
                unique_pipeline_id=placeholder_utils.make_random_id(),
            )
        else:
            raise ValueError(
                'Got unknown spec in ExecutorSpec. Only dsl.component and dsl.container_component are supported in local pipeline execution.'
            )

        if task_status == status.Status.FAILURE:
            maybe_raise_on_failure(task_status)

        # handle failure
        # update IO store before next task
        for key, output in outputs.items():
            io_store.put_task_output(
                task_name,
                key,
                output,
            )
    msg = f'Pipeline {pipeline_name_with_color} finished with status {status_with_color}'
    with logging_utils.local_logger_context():
        logging.info(msg)
    ...
    return status.Status.SUCCESS


def maybe_raise_on_failure(
    pipeline_name: str,
    task_name: str,
    raise_on_error: bool,
):
    pipeline_name_with_color = logging_utils.format_pipeline_name(pipeline_name)
    task_name_with_color = logging_utils.format_task_name(task_name)
    status_with_color = logging_utils.format_status(status.Status.FAILURE.name)
    msg = f'Pipeline {pipeline_name_with_color} finished with status {status_with_color}. Inner task failed: {task_name_with_color}.'
    if raise_on_error:
        raise RuntimeError(msg)
    with logging_utils.local_logger_context():
        logging.error(msg)


def bind_defaults_to_dag_arguments(
    dag_arguments: Dict[str, Any],
    dag_inputs_spec: pipeline_spec_pb2.ComponentInputsSpec,
) -> Dict[str, Any]:
    """For each required argument in dag_arguments which is missing, adds the
    default argument.

    Args:
        dag_arguments: The user-provided arguments to the DAG.
        dag_inputs_spec: The ComponentInputSpec for the DAG.

    Returns:
        The complete DAG inputs, with defaults included where the user-provided argument is missing.
    """
    from kfp.local import executor_output_utils

    dag_arguments_with_defaults = {}
    for input_name, input_spec in dag_inputs_spec.parameters.items():
        if input_name not in dag_arguments:
            dag_arguments_with_defaults[
                input_name] = executor_output_utils.pb2_value_to_python(
                    input_spec.default_value)
        else:
            dag_arguments_with_defaults[input_name] = dag_arguments[input_name]
    return dag_arguments_with_defaults


def make_task_arguments(
    task_inputs_spec: pipeline_spec_pb2.TaskInputsSpec,
    io_store: io.IOStore,
) -> Dict[str, Any]:
    """Obtains a dictionary of arguments required to execute the task
    corresponding to TaskInputsSpec.

    Args:
        task_inputs_spec: The TaskInputsSpec for the task for which we are obtaining task arguments.
        io_store: The IOStore of the current DAG. Used to task arguments which come from upstream task outputs and parent component inputs.

    Returns:
        The arguments for the task.
    """
    from kfp.local import executor_output_utils

    task_arguments = {}
    # handle parameters
    for input_name, input_spec in task_inputs_spec.inputs.parameters.items():
        # handle upstream outputs
        if input_spec.HasField('task_output_parameter'):
            task_arguments[input_name] = io_store.get_task_output(
                input_spec.task_output_parameter.producer_task,
                input_spec.task_output_parameter.output_parameter_key,
            )

        # handle constants
        elif input_spec.HasField('runtime_value'):
            if input_spec.runtime_value.WhichOneof('value') != 'constant':
                raise ValueError('Expected constant.')
            task_arguments[
                input_name] = executor_output_utils.pb2_value_to_python(
                    input_spec.runtime_value.constant)

        # handle parent pipeline input parameters
        elif input_spec.HasField('component_input_parameter'):
            task_arguments[input_name] = io_store.get_parent_input(
                input_spec.component_input_parameter)
        elif input_spec.HasField('task_final_status'):
            raise ValueError(
                "'dsl.ExitHandler' is not yet support for local execution.")
        else:
            raise ValueError(f'Invalid or missing input for {input_name}')
    # handle artifacts
    for input_name, input_spec in task_inputs_spec.inputs.artifacts.items():
        if input_spec.HasField('task_output_artifact'):
            task_arguments[input_name] = io_store.get_task_output(
                input_spec.task_output_artifact.producer_task,
                input_spec.task_output_artifact.output_artifact_key,
            )
        elif input_spec.HasField('component_input_artifact'):
            task_arguments[input_name] = io_store.get_parent_input(
                input_spec.component_input_artifact)
        else:
            raise ValueError(f'Invalid or missing input for {input_name}')

    return task_arguments


def get_dag_output_parameters(
    dag_outputs_spec: pipeline_spec_pb2.DagOutputsSpec,
    io_store: io.IOStore,
) -> Dict[str, Any]:
    """Gets the DAG output parameters from a DagOutputsSpec and IOStore
    corresponding to the DAG.

    Args:
        dag_outputs_spec: DagOutputsSpec corresponding to the DAG.
        io_store: IOStore corresponding to the DAG.

    Returns:
        The DAG output parameters.
    """
    outputs = {}
    for root_output_key, parameter_selector_spec in dag_outputs_spec.parameters.items(
    ):
        kind = parameter_selector_spec.WhichOneof('kind')
        if kind == 'value_from_parameter':
            value_from_parameter = parameter_selector_spec.value_from_parameter
            outputs[root_output_key] = io_store.get_task_output(
                value_from_parameter.producer_subtask,
                value_from_parameter.output_parameter_key,
            )
        elif kind == 'value_from_oneof':
            raise ValueError(
                "'dsl.OneOf' is not yet supported in local execution.")
        else:
            raise ValueError(
                f"Got unknown 'parameter_selector_spec' kind: {kind}")
    return outputs


def get_dag_output_artifacts(
    dag_outputs_spec: pipeline_spec_pb2.DagOutputsSpec,
    io_store: io.IOStore,
) -> Dict[str, Any]:
    """Gets the DAG output artifacts from a DagOutputsSpec and IOStore
    corresponding to the DAG.

    Args:
        dag_outputs_spec: DagOutputsSpec corresponding to the DAG.
        io_store: IOStore corresponding to the DAG.

    Returns:
        The DAG output artifacts.
    """
    outputs = {}
    for root_output_key, artifact_selector_spec in dag_outputs_spec.artifacts.items(
    ):
        len_artifact_selectors = len(artifact_selector_spec.artifact_selectors)
        if len_artifact_selectors != 1:
            raise ValueError(
                f'Expected 1 artifact in ArtifactSelectorSpec. Got: {len_artifact_selectors}'
            )
        artifact_selector = artifact_selector_spec.artifact_selectors[0]
        outputs[root_output_key] = io_store.get_task_output(
            artifact_selector.producer_subtask,
            artifact_selector.output_artifact_key,
        )
    return outputs
