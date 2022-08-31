# Copyright 2020 The Kubeflow Authors
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
"""Utility function for building Importer Node spec."""

from typing import Any, Tuple, Union, Optional, Type, Mapping, Dict, List
from google.protobuf import struct_pb2
import sys
import copy

from kfp.dsl import _container_op
from kfp.dsl import _pipeline_param
from kfp.dsl import dsl_utils
from kfp.pipeline_spec import pipeline_spec_pb2
from kfp.v2.components.types import artifact_types, type_utils

URI_INPUT_KEY = 'uri'
ARTIFACT_OUTPUT_KEY = 'artifact'


def make_input_parameter_placeholder(key: str) -> str:
    return f"{{{{$.inputs.parameters['{key}']}}}}"


def make_placeholder_unique(
    name: str,
    collection: List[str],
    delimiter: str,
) -> str:
    """Makes a unique name by adding index."""
    unique_name = name
    if unique_name in collection:
        for i in range(2, sys.maxsize**10):
            unique_name = name + delimiter + str(i)
            if unique_name not in collection:
                break
    return unique_name


def transform_metadata_and_get_inputs(
    metadata: Dict[Union[str, _pipeline_param.PipelineParam],
                   Union[_pipeline_param.PipelineParam, Any]]
) -> Tuple[Dict[str, Any], List[_pipeline_param.PipelineParam]]:

    def traverse_dict_and_create_metadata_inputs(d: Any) -> Any:
        if isinstance(d, _pipeline_param.PipelineParam):
            unique_name = make_placeholder_unique(
                d.name,
                input_keys,
                '-',
            )
            new_pipeline_param = copy.deepcopy(d)
            new_pipeline_param.name = unique_name
            input_keys.append(unique_name)
            inputs.append(new_pipeline_param)

            return make_input_parameter_placeholder(unique_name)
        elif isinstance(d, dict):
            return {
                traverse_dict_and_create_metadata_inputs(k):
                traverse_dict_and_create_metadata_inputs(v)
                for k, v in d.items()
            }

        elif isinstance(d, list):
            return [traverse_dict_and_create_metadata_inputs(el) for el in d]
        else:
            return d

    inputs: List[_pipeline_param.PipelineParam] = []
    input_keys: List[str] = []
    new_metadata = traverse_dict_and_create_metadata_inputs(metadata)
    return new_metadata, inputs


def _build_importer_spec(
    artifact_uri: Union[_pipeline_param.PipelineParam, str],
    artifact_type_schema: pipeline_spec_pb2.ArtifactTypeSchema,
    metadata_with_placeholders: Optional[Mapping[str, Any]] = None,
) -> pipeline_spec_pb2.PipelineDeploymentConfig.ImporterSpec:
    """Builds an importer executor spec.

    Args:
      artifact_uri: The artifact uri to import from.
      artifact_type_schema: The user specified artifact type schema of the
        artifact to be imported.
      metadata_with_placeholders: Metadata dictionary with pipeline parameters replaced with string placeholders.

    Returns:
      An importer spec.
    """

    importer_spec = pipeline_spec_pb2.PipelineDeploymentConfig.ImporterSpec()
    importer_spec.type_schema.CopyFrom(artifact_type_schema)

    if isinstance(artifact_uri, _pipeline_param.PipelineParam):
        importer_spec.artifact_uri.runtime_parameter = URI_INPUT_KEY
    elif isinstance(artifact_uri, str):
        importer_spec.artifact_uri.constant_value.string_value = artifact_uri

    if metadata_with_placeholders:
        metadata_protobuf_struct = struct_pb2.Struct()
        metadata_protobuf_struct.update(metadata_with_placeholders)
        importer_spec.metadata.CopyFrom(metadata_protobuf_struct)

    return importer_spec


def _build_importer_task_spec(
    artifact_uri: Union[_pipeline_param.PipelineParam, str],
    importer_base_name: str,
    metadata_inputs: List[_pipeline_param.PipelineParam],
) -> pipeline_spec_pb2.PipelineTaskSpec:
    """Builds an importer task spec.

    Args:
      importer_base_name: The base name of the importer node.
      inputs: Task inputs.

    Returns:
      An importer node task spec.
    """
    result = pipeline_spec_pb2.PipelineTaskSpec()
    result.component_ref.name = dsl_utils.sanitize_component_name(
        importer_base_name)

    for param in metadata_inputs:
        if param.op_name:
            result.inputs.parameters[
                param.name].task_output_parameter.producer_task = (
                    dsl_utils.sanitize_task_name(param.op_name))
            result.inputs.parameters[
                param
                .name].task_output_parameter.output_parameter_key = param.name
        else:
            result.inputs.parameters[
                param.name].component_input_parameter = param.full_name

    if isinstance(artifact_uri, _pipeline_param.PipelineParam):
        param = artifact_uri
        if param.op_name:
            result.inputs.parameters[
                URI_INPUT_KEY].task_output_parameter.producer_task = (
                    dsl_utils.sanitize_task_name(param.op_name))
            result.inputs.parameters[
                URI_INPUT_KEY].task_output_parameter.output_parameter_key = param.name
        else:
            result.inputs.parameters[
                URI_INPUT_KEY].component_input_parameter = param.full_name
    elif isinstance(artifact_uri, str):
        result.inputs.parameters[
            URI_INPUT_KEY].runtime_value.constant_value.string_value = artifact_uri

    return result


def _build_importer_component_spec(
    artifact_uri: Union[_pipeline_param.PipelineParam, str],
    importer_base_name: str,
    artifact_type_schema: pipeline_spec_pb2.ArtifactTypeSchema,
    metadata_inputs: List[_pipeline_param.PipelineParam],
) -> pipeline_spec_pb2.ComponentSpec:
    """Builds an importer component spec.

    Args:
      importer_base_name: The base name of the importer node.
      artifact_type_schema: The user specified artifact type schema of the
        artifact to be imported.
      inputs: Task inputs.

    Returns:
      An importer node component spec.
    """
    result = pipeline_spec_pb2.ComponentSpec()
    result.executor_label = dsl_utils.sanitize_executor_label(
        importer_base_name)

    for param in metadata_inputs:
        result.input_definitions.parameters[
            param.name].type = type_utils._PARAMETER_TYPES_MAPPING.get(
                param.param_type.lower())

    result.input_definitions.parameters[
        URI_INPUT_KEY].type = pipeline_spec_pb2.PrimitiveType.STRING

    result.output_definitions.artifacts[
        ARTIFACT_OUTPUT_KEY].artifact_type.CopyFrom(artifact_type_schema)

    return result


def importer(
        artifact_uri: Union[_pipeline_param.PipelineParam, str],
        artifact_class: Type[artifact_types.Artifact],
        reimport: bool = False,
        metadata: Optional[Mapping[str,
                                   Any]] = None) -> _container_op.ContainerOp:
    """dsl.importer for importing an existing artifact. Only for v2 pipeline.

    Args:
      artifact_uri: The artifact uri to import from.
      artifact_type_schema: The user specified artifact type schema of the
        artifact to be imported.
      reimport: Whether to reimport the artifact. Defaults to False.

    Returns:
      A ContainerOp instance.

    Raises:
      ValueError if the passed in artifact_uri is neither a PipelineParam nor a
        constant string value.
    """

    old_warn_value = _container_op.ContainerOp._DISABLE_REUSABLE_COMPONENT_WARNING
    _container_op.ContainerOp._DISABLE_REUSABLE_COMPONENT_WARNING = True
    task = _container_op.ContainerOp(
        name='importer',
        image='importer_image',  # TODO: need a v1 implementation of importer.
        arguments=[],
        file_outputs={
            ARTIFACT_OUTPUT_KEY:
                "{{{{$.outputs.artifacts['{}'].uri}}}}".format(
                    ARTIFACT_OUTPUT_KEY)
        },
    )
    _container_op.ContainerOp._DISABLE_REUSABLE_COMPONENT_WARNING = old_warn_value
    artifact_type_schema = type_utils.get_artifact_type_schema(artifact_class)
    metadata_with_placeholders, metadata_inputs = transform_metadata_and_get_inputs(
        metadata)

    task.importer_spec = _build_importer_spec(
        artifact_uri=artifact_uri,
        artifact_type_schema=artifact_type_schema,
        metadata_with_placeholders=metadata_with_placeholders)

    task.task_spec = _build_importer_task_spec(
        artifact_uri=artifact_uri,
        importer_base_name=task.name,
        metadata_inputs=metadata_inputs)

    task.component_spec = _build_importer_component_spec(
        artifact_uri=artifact_uri,
        importer_base_name=task.name,
        artifact_type_schema=artifact_type_schema,
        metadata_inputs=metadata_inputs)
    task.inputs = metadata_inputs

    if isinstance(artifact_uri, _pipeline_param.PipelineParam):
        input_artifact_uri = artifact_uri
    elif isinstance(artifact_uri, str):
        input_artifact_uri = _pipeline_param.PipelineParam(
            name=URI_INPUT_KEY, value=artifact_uri, param_type='String')

    task.inputs.append(input_artifact_uri)
    print(task)
    return task
