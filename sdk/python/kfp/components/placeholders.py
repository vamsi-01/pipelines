# Copyright 2022 The Kubeflow Authors
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
"""Contains data structures and functions for handling input and output
placeholders."""

# import abc
# import dataclasses
# import json
# from json.decoder import JSONArray  # type: ignore
# from json.scanner import py_make_scanner
# import re
from typing import Any, Dict, List, Optional, Union

# from kfp.components import base_model
# from kfp.components import utils
# from kfp.components.types import type_utils


def ExecutorInputPlaceholder() -> str:
    return '{{$}}'


def InputValuePlaceholder(input_name: str) -> str:
    return f"{{{{$.inputs.parameters['{input_name}']}}}}"


def InputPathPlaceholder(input_name: str) -> str:
    return f"{{{{$.inputs.artifacts['{input_name}'].path}}}}"


def InputUriPlaceholder(input_name: str) -> str:
    return f"{{{{$.inputs.artifacts['{input_name}'].uri}}}}"


def InputMetadataPlaceholder(input_name: str) -> str:
    return f"{{{{$.inputs.artifacts['{input_name}'].metadata}}}}"


def OutputParameterPlaceholder(output_name: str) -> str:
    return f"{{{{$.outputs.parameters['{output_name}'].output_file}}}}"


def OutputPathPlaceholder(output_name: str) -> str:
    return f"{{{{$.outputs.artifacts['{output_name}'].path}}}}"


def OutputUriPlaceholder(output_name: str) -> str:
    return f"{{{{$.outputs.artifacts['{output_name}'].uri}}}}"


def OutputMetadataPlaceholder(output_name: str) -> str:
    return f"{{{{$.outputs.artifacts['{output_name}'].metadata}}}}"


def ConcatPlaceholder(items: List[str]) -> Dict[str, List[str]]:
    return {'Concat': items}


def IfPresentPlaceholder(
    input_name: str,
    then: List[str],
    else_: Optional[List[str]] = None
) -> Dict[str, Dict[str, Union[str, List[str]]]]:
    if not else_:
        return {'IfPresent': {'InputName': input_name, 'Then': then}}
    return {'IfPresent': {'InputName': input_name, 'Then': then, 'Else': else_}}

def convert_placeholder struct

# def maybe_convert_v1_yaml_placeholder_to_v2_placeholder_str(
#     arg: Dict[str, Any],
#     component_dict: Dict[str,
#                          Any]) -> Union[Dict[str, Any], CommandLineElement]:
#     if isinstance(arg, str):
#         return arg

#     if not isinstance(arg, dict):
#         raise ValueError

#     has_one_entry = len(arg) == 1

#     if not has_one_entry:
#         raise ValueError(
#             f'Got unexpected dictionary {arg}. Expected a dictionary with one entry.'
#         )

#     first_key = list(arg.keys())[0]
#     first_value = list(arg.values())[0]
#     if first_key == 'inputValue':
#         return InputValuePlaceholder(
#             input_name=utils.sanitize_input_name(
#                 first_value))._to_placeholder_string()

#     elif first_key == 'inputPath':
#         return InputPathPlaceholder(
#             input_name=utils.sanitize_input_name(
#                 first_value))._to_placeholder_string()

#     elif first_key == 'inputUri':
#         return InputUriPlaceholder(
#             input_name=utils.sanitize_input_name(
#                 first_value))._to_placeholder_string()

#     elif first_key == 'outputPath':
#         outputs = component_dict['outputs']
#         for output in outputs:
#             if output['name'] == first_value:
#                 type_ = output.get('type')
#                 is_parameter = type_ is None or (
#                     isinstance(type_, str) and
#                     type_.lower() in type_utils._PARAMETER_TYPES_MAPPING)
#                 if is_parameter:
#                     return OutputParameterPlaceholder(
#                         output_name=utils.sanitize_input_name(
#                             first_value))._to_placeholder_string()
#                 else:
#                     return OutputPathPlaceholder(
#                         output_name=utils.sanitize_input_name(
#                             first_value))._to_placeholder_string()
#         raise ValueError(
#             f'{first_value} not found in component outputs. Could not process placeholders. Component spec: {component_dict}.'
#         )

#     elif first_key == 'outputUri':
#         return OutputUriPlaceholder(
#             output_name=utils.sanitize_input_name(
#                 first_value))._to_placeholder_string()

#     elif first_key == 'ifPresent':
#         structure_kwargs = arg['ifPresent']
#         structure_kwargs['input_name'] = structure_kwargs.pop('inputName')
#         structure_kwargs['otherwise'] = structure_kwargs.pop('else')
#         structure_kwargs['then'] = [
#             maybe_convert_v1_yaml_placeholder_to_v2_placeholder_str(
#                 e, component_dict=component_dict)
#             for e in structure_kwargs['then']
#         ]
#         structure_kwargs['otherwise'] = [
#             maybe_convert_v1_yaml_placeholder_to_v2_placeholder_str(
#                 e, component_dict=component_dict)
#             for e in structure_kwargs['otherwise']
#         ]
#         return IfPresentPlaceholder(**structure_kwargs)._to_placeholder_string()

#     elif first_key == 'concat':
#         return ConcatPlaceholder(items=[
#             maybe_convert_v1_yaml_placeholder_to_v2_placeholder_str(
#                 e, component_dict=component_dict) for e in arg['concat']
#         ])._to_placeholder_string()

#     elif first_key == 'executorInput':
#         return ExecutorInputPlaceholder()._to_placeholder_string()

#     elif 'if' in arg:
#         if_ = arg['if']
#         input_name = utils.sanitize_input_name(if_['cond']['isPresent'])
#         then_ = if_['then']
#         else_ = if_.get('else', [])
#         return IfPresentPlaceholder(
#             input_name=input_name,
#             then=[
#                 maybe_convert_v1_yaml_placeholder_to_v2_placeholder_str(
#                     val, component_dict=component_dict) for val in then_
#             ],
#             else_=[
#                 maybe_convert_v1_yaml_placeholder_to_v2_placeholder_str(
#                     val, component_dict=component_dict) for val in else_
#             ])._to_placeholder_string()

#     elif 'concat' in arg:

#         return ConcatPlaceholder(items=[
#             maybe_convert_v1_yaml_placeholder_to_v2_placeholder_str(
#                 val, component_dict=component_dict) for val in arg['concat']
#         ])._to_placeholder_string()
#     else:
#         raise TypeError(f'Unexpected argument {arg} of type {type(arg)}.')
