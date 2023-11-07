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

from typing import Any, Dict, List, Union

from google.protobuf import json_format
from google.protobuf import struct_pb2
from kfp import dsl
from kfp.pipeline_spec import pipeline_spec_pb2


class LocalTask:
    """A pipeline task which was executed locally.

    This is the result of a locally invoked component.
    """

    def __init__(self, outputs: Dict[str, Any]) -> None:
        """Constructs a LocalTask.

        Args:
            outputs: The dictionary of task outputs.
        """
        self._outputs = outputs

    # TODO: make private
    @staticmethod
    def from_messages(
        executor_input: pipeline_spec_pb2.ExecutorInput,
        component_spec: pipeline_spec_pb2.ComponentSpec,
    ):
        """Constructs a LocalTask from the ExecutorInput and ComponentSpec
        messages.

        Args:
            executor_input: ExecutorInput corresponding to the executed task.
            component_spec: ExecutorInput corresponding to the executed component.

        Returns:
            A LocalTask instance.
        """
        executor_output = load_executor_output(
            executor_output_path=executor_input.outputs.output_file)
        executor_output = add_type_to_executor_output(
            executor_input=executor_input,
            executor_output=executor_output,
        )
        outputs = get_outputs_from_executor_output(
            executor_output=executor_output,
            component_spec=component_spec,
        )
        return LocalTask(outputs=outputs)

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


def load_executor_output(
    executor_output_path: str,) -> pipeline_spec_pb2.ExecutorOutput:
    """Loads the ExecutorOutput message from a path.

    Args:
        executor_output_path: The file path.

    Returns:
        The ExecutorOutput message.
    """
    executor_output = pipeline_spec_pb2.ExecutorOutput()
    with open(executor_output_path) as f:
        json_format.Parse(f.read(), executor_output)
    return executor_output


def get_outputs_from_executor_output(
    executor_output: pipeline_spec_pb2.ExecutorOutput,
    component_spec: pipeline_spec_pb2.ComponentSpec,
) -> Dict[str, Any]:
    parameters = {
        param_name: pb2_value_to_python(value)
        for param_name, value in executor_output.parameter_values.items()
    }
    output_artifacts = component_spec.output_definitions.artifacts
    artifacts = {
        artifact_name: artifact_list_to_dsl_artifact(
            artifact_list,
            is_artifact_list=output_artifacts[artifact_name].is_artifact_list,
        ) for artifact_name, artifact_list in executor_output.artifacts.items()
    }
    return {**parameters, **artifacts}


def pb2_value_to_python(value: struct_pb2.Value) -> Any:
    """Converts protobuf Value to the corresponding Python type."""
    if value.HasField('null_value'):
        return None
    elif value.HasField('number_value'):
        return value.number_value
    elif value.HasField('string_value'):
        return value.string_value
    elif value.HasField('bool_value'):
        return value.bool_value
    elif value.HasField('struct_value'):
        return pb2_struct_to_python(value.struct_value)
    elif value.HasField('list_value'):
        return [pb2_value_to_python(v) for v in value.list_value.values]
    else:
        raise ValueError(f'Unknown value type: {value}')


def pb2_struct_to_python(struct):
    """Converts protobuf Struct to a dict."""
    return {k: pb2_value_to_python(v) for k, v in struct.fields.items()}


def runtime_artifact_to_dsl_artifact(
        runtime_artifact: pipeline_spec_pb2.RuntimeArtifact) -> dsl.Artifact:
    """Converts a single RuntimeArtifact instance to the corresponding
    dsl.Artifact instance."""
    from kfp.dsl import executor
    return executor.create_artifact_instance(
        json_format.MessageToDict(runtime_artifact))


def artifact_list_to_dsl_artifact(
    artifact_list: pipeline_spec_pb2.ArtifactList,
    is_artifact_list: bool,
) -> Union[dsl.Artifact, List[dsl.Artifact]]:
    """Converts an ArtifactList instance to a single dsl.Artifact or a list of
    dsl.Artifacts, depending on thether the ArtifactList is a true list or
    simply a container for single element."""
    dsl_artifacts = [
        runtime_artifact_to_dsl_artifact(artifact_spec)
        for artifact_spec in artifact_list.artifacts
    ]
    return dsl_artifacts if is_artifact_list else dsl_artifacts[0]


# TODO: we don't really need to do this -- we can always just map data from ExecutorInput
def add_type_to_executor_output(
    executor_input: pipeline_spec_pb2.ExecutorInput,
    executor_output: pipeline_spec_pb2.ExecutorOutput,
) -> pipeline_spec_pb2.ExecutorOutput:
    """Adds artifact type information (ArtifactTypeSchema) from the
    ExecutorInput message to the ExecutorOutput message.

    This information is not present in the serialized ExecutorOutput message, though it would be useful to have it for constructing LocalTask outputs (accessed via task.outputs['foo']). We don't want to change the serialized output, however, and introduce differences between local and cloud execution.

    To simplify the local implementation, we add this extra info to ExecutorOutput.
    """
    for key, artifact_list in executor_output.artifacts.items():
        for artifact in artifact_list.artifacts:
            artifact.type.CopyFrom(
                executor_input.outputs.artifacts[key].artifacts[0].type)
    return executor_output
