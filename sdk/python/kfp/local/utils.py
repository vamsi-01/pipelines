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
"""Utilities shared across local runners."""
import os
import re
from typing import Any, Dict, List

from google.protobuf import json_format
# TODO: nest
from kfp.compiler import pipeline_spec_builder
from kfp.pipeline_spec import pipeline_spec_pb2


def construct_executor_input(
    component_spec: pipeline_spec_pb2.ComponentSpec,
    arguments: Dict[str, Any],
    pipeline_root: str,
) -> pipeline_spec_pb2.ExecutorInput:
    input_parameter_keys = list(
        component_spec.input_definitions.parameters.keys())
    input_artifact_keys = list(
        component_spec.input_definitions.artifacts.keys())
    # TODO: support input artifact constants
    if input_artifact_keys:
        raise ValueError(
            'Input artifacts are not yet supported for local execution.')

    output_parameter_keys = list(
        component_spec.output_definitions.parameters.keys())
    output_artifact_specs_dict = component_spec.output_definitions.artifacts

    inputs = pipeline_spec_pb2.ExecutorInput.Inputs(
        parameter_values={
            param_name:
            pipeline_spec_builder.to_protobuf_value(arguments[param_name])
            if param_name in arguments else component_spec.input_definitions
            .parameters[param_name].default_value
            for param_name in input_parameter_keys
        },
        # TODO: support input artifact constants
        artifacts={},
    )
    outputs = pipeline_spec_pb2.ExecutorInput.Outputs(
        parameters={
            param_name: pipeline_spec_pb2.ExecutorInput.OutputParameter(
                output_file=os.path.join(pipeline_root, param_name))
            for param_name in output_parameter_keys
        },
        artifacts={
            artifact_name: make_artifact_list(
                name=artifact_name,
                artifact_type=artifact_spec.artifact_type,
                pipeline_root=pipeline_root,
            ) for artifact_name, artifact_spec in
            output_artifact_specs_dict.items()
        },
        # TODO: use constant for executor_output.json?
        output_file=os.path.join(pipeline_root, 'executor_output.json'),
    )
    return pipeline_spec_pb2.ExecutorInput(
        inputs=inputs,
        outputs=outputs,
    )


def make_artifact_list(
    name: str,
    artifact_type: pipeline_spec_pb2.ArtifactTypeSchema,
    pipeline_root: str,
) -> pipeline_spec_pb2.ArtifactList:
    return pipeline_spec_pb2.ArtifactList(artifacts=[
        pipeline_spec_pb2.RuntimeArtifact(
            name=name,
            type=artifact_type,
            uri=os.path.join(pipeline_root, name),
            # metadata always starts empty for output artifacts
            metadata={},
        )
    ])


def replace_placeholders(
    full_command: List[str],
    executor_input: str,
) -> List[str]:
    return [
        replace_placeholder_for_element(el, executor_input)
        for el in full_command
    ]


def replace_placeholder_for_element(
    element: str,
    executor_input: pipeline_spec_pb2.ExecutorInput,
) -> str:
    PLACEHOLDER_MAP: Dict[str, str] = {'{{$}}': executor_input}
    for placeholder, resolved in PLACEHOLDER_MAP.items():
        element = element.replace(placeholder,
                                  json_format.MessageToJson(resolved))
    return element


def extract_content_inside_placeholders(text):
    # Corrected regex pattern to match the content inside the placeholders
    pattern = r'\{\{\$\.(.*?)\}\}'

    return list(re.finditer(pattern, text))


# TODO: validate permitted artifacts


def process_match(
    placeholder_body: str,
    executor_input: pipeline_spec_pb2.ExecutorInput,
) -> str:
    resolved_output = executor_input
    placeholder_parts = placeholder_body.split('.')
    for field in placeholder_parts:
        if field.endswith(']'):
            matches = list(re.finditer(r"\[\'\'(.*)\'\'\]", field))
            match = matches[0]
            if match.span()[1] != len(field):
                raise ValueError(
                    'Got unexpected match when replacing placeholders.')

            field = field[:match.span()[0]]
            key = match.group(1)

            resolved_output = getattr(resolved_output, field)
            resolved_output = resolved_output[key]
            if field == 'artifacts':
                resolved_output = resolved_output.artifacts[0]

        else:
            print('FIELD', field)
            resolved_output = getattr(resolved_output, field)

    return json_format.MessageToJson(resolved_output)


def replace_placeholder_constants(
        element: str, executor_input: pipeline_spec_pb2.ExecutorInput) -> str:
    constants = {
        r'{{$.outputs.output_file}}': executor_input.outputs.output_file,
        r'{{$.outputMetadataUri}}': executor_input.outputs.output_file,
        r'{{$.pipeline_job_name}}': 'foo',
        r'{{$.pipeline_job_uuid}}': 'foo',
        r'{{$.pipeline_task_name}}': 'foo',
        r'{{$.pipeline_task_uuid}}': 'foo',
        r'{{$.pipeline_root}}': 'foo',
    }

    for placeholder, value in constants.items():
        element = element.replace(placeholder, value)

    return element.replace(r'{{$}}', json_format.MessageToJson(executor_input))


def replace_placholder(
    element: str,
    executor_input: pipeline_spec_pb2.ExecutorInput,
) -> str:
    element = replace_placeholder_constants(element, executor_input)
    matches = extract_content_inside_placeholders(element)
    if not matches:
        return element
    match = matches[0]
    print(match.group(0))

    if match.group(0) == r'{{$}}':
        return json_format.MessageToJson(executor_input)

    # TODO: >1 match
    start = match.span()[0]
    end = match.span()[1]
    return element[:start] + process_match(match.group(1),
                                           executor_input) + element[end + 1:]
