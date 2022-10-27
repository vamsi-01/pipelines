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
# from typing import Any, Dict, List, Optional, Union

# from kfp.components import base_model
# from kfp.components import utils
# from kfp.components.types import type_utils


def ExecutorInputPlaceholder():
    return '{{$}}'


def InputValuePlaceholder(input_name: str):
    return f"{{{{$.inputs.parameters['{input_name}']}}}}"


def InputPathPlaceholder(input_name: str):
    return f"{{{{$.inputs.artifacts['{input_name}'].path}}}}"


def InputUriPlaceholder(input_name: str):
    return f"{{{{$.inputs.artifacts['{input_name}'].uri}}}}"


class InputMetadataPlaceholder:

    def __new__(cls, input_name: str):
        return f"{{{{$.inputs.artifacts['{input_name}'].metadata}}}}"


class OutputParameterPlaceholder:

    def __new__(cls, output_name: str):
        return f"{{{{$.outputs.parameters['{output_name}'].output_file}}}}"


# class OutputParameterPlaceholder(base_model.BaseModel,
#                                  RegexPlaceholderSerializationMixin):
#     """Class that holds an output parameter placeholder.

#     Attributes:
#         output_name: Name of the output.
#     """
#     output_name: str
#     _aliases = {'output_name': 'outputPath'}
#     _TO_PLACEHOLDER = "{{{{$.outputs.parameters['{output_name}'].output_file}}}}"
#     _FROM_PLACEHOLDER = re.compile(
#         r"^\{\{\$\.outputs\.parameters\[(?:''|'|\")(?P<output_name>.+?)(?:''|'|\")]\.output_file\}\}$"
#     )

# class OutputPathPlaceholder(base_model.BaseModel,
#                             RegexPlaceholderSerializationMixin):
#     """Class that holds an output path placeholder.

#     Attributes:
#         output_name: Name of the output.
#     """
#     output_name: str
#     _aliases = {'output_name': 'outputPath'}
#     _TO_PLACEHOLDER = "{{{{$.outputs.artifacts['{output_name}'].path}}}}"
#     _FROM_PLACEHOLDER = re.compile(
#         r"^\{\{\$\.outputs\.artifacts\[(?:''|'|\")(?P<output_name>.+?)(?:''|'|\")]\.path\}\}$"
#     )

# class OutputUriPlaceholder(base_model.BaseModel,
#                            RegexPlaceholderSerializationMixin):
#     """Class that holds output uri for conditional cases.

#     Attributes:
#         output_name: name of the output.
#     """
#     output_name: str
#     _aliases = {'output_name': 'outputUri'}
#     _TO_PLACEHOLDER = "{{{{$.outputs.artifacts['{output_name}'].uri}}}}"
#     _FROM_PLACEHOLDER = re.compile(
#         r"^\{\{\$\.outputs\.artifacts\[(?:''|'|\")(?P<output_name>.+?)(?:''|'|\")]\.uri\}\}$"
#     )

# class OutputMetadataPlaceholder(base_model.BaseModel,
#                                 RegexPlaceholderSerializationMixin):
#     """Class that holds an output metadata placeholder.

#     Attributes:
#         output_name: Name of the output.
#     """
#     output_name: str
#     _aliases = {'output_name': 'outputMetadata'}
#     _TO_PLACEHOLDER = "{{{{$.outputs.artifacts['{output_name}'].metadata}}}}"
#     _FROM_PLACEHOLDER = re.compile(
#         r"^\{\{\$\.outputs\.artifacts\[(?:''|'|\")(?P<output_name>.+?)(?:''|'|\")]\.metadata\}\}$"
#     )

# CommandLineElement = Union[str, ExecutorInputPlaceholder, InputValuePlaceholder,
#                            InputPathPlaceholder, InputUriPlaceholder,
#                            OutputParameterPlaceholder, OutputPathPlaceholder,
#                            OutputUriPlaceholder, 'IfPresentPlaceholder',
#                            'ConcatPlaceholder']

# class ConcatPlaceholder(base_model.BaseModel, Placeholder):
#     """Placeholder for concatenating multiple strings. May contain other
#     placeholders.

#     Examples:
#       ::

#         @container_component
#         def container_with_concat_placeholder(text1: str, text2: Output[Dataset],
#                                               output_path: OutputPath(str)):
#             return ContainerSpec(
#                 image='python:3.7',
#                 command=[
#                     'my_program',
#                     ConcatPlaceholder(['prefix-', text1, text2.uri])
#                 ],
#                 args=['--output_path', output_path]
#             )
#     """
#     items: List[CommandLineElement]
#     """Elements to concatenate."""

#     @classmethod
#     def _split_cel_concat_string(self, string: str) -> List[str]:
#         """Splits a cel string into a list of strings, which may be normal
#         strings or placeholder strings.

#         Args:
#             cel_string (str): The cel string.

#         Returns:
#             List[str]: The list of strings.
#         """
#         concat_char = '+'
#         start_ends = [(match.start(0), match.end(0)) for match in
#                       InputValuePlaceholder._FROM_PLACEHOLDER.finditer(string)]

#         items = []
#         if start_ends:
#             start = 0
#             for match_start, match_end in start_ends:
#                 leading_string = string[start:match_start]
#                 if leading_string and leading_string != concat_char:
#                     items.append(leading_string)
#                 items.append(string[match_start:match_end])
#                 start = match_end
#             trailing_string = string[match_end:]
#             if trailing_string and trailing_string != concat_char:
#                 items.append(trailing_string)
#         return items

#     @classmethod
#     def _is_match(cls, placeholder_string: str) -> bool:
#         # 'Concat' is the explicit struct for concatenation
#         # cel splitting handles the cases of {{input}}+{{input}} and {{input}}otherstring
#         return 'Concat' in json_load_nested_placeholder_aware(
#             placeholder_string
#         ) or len(
#             ConcatPlaceholder._split_cel_concat_string(placeholder_string)) > 1

#     def _to_placeholder_struct(self) -> Dict[str, Any]:
#         return {
#             'Concat': [
#                 maybe_convert_placeholder_to_placeholder_string(item)
#                 for item in self.items
#             ]
#         }

#     def _to_placeholder_string(self) -> str:
#         return json.dumps(self._to_placeholder_struct())

#     @classmethod
#     def _from_placeholder_string(
#             cls, placeholder_string: str) -> 'ConcatPlaceholder':
#         placeholder_struct = json_load_nested_placeholder_aware(
#             placeholder_string)
#         if isinstance(placeholder_struct, str):
#             items = [
#                 maybe_convert_placeholder_string_to_placeholder(item)
#                 for item in cls._split_cel_concat_string(placeholder_struct)
#             ]
#             return cls(items=items)
#         elif isinstance(placeholder_struct, dict):
#             items = [
#                 maybe_convert_placeholder_string_to_placeholder(item)
#                 for item in placeholder_struct['Concat']
#             ]
#             return ConcatPlaceholder(items=items)

#         raise ValueError

# class IfPresentPlaceholder(base_model.BaseModel, Placeholder):
#     """Placeholder for handling cases where an input may or may not be passed.
#     May contain other placeholders.

#     Examples:
#       ::

#         @container_component
#         def container_with_if_placeholder(output_path: OutputPath(str),
#                                           dataset: Output[Dataset],
#                                           optional_input: str = 'default'):
#             return ContainerSpec(
#                     image='python:3.7',
#                     command=[
#                         'my_program',
#                         IfPresentPlaceholder(
#                             input_name='optional_input',
#                             then=[optional_input],
#                             else_=['no_input']), '--dataset',
#                         IfPresentPlaceholder(
#                             input_name='optional_input', then=[dataset.uri], else_=['no_dataset'])
#                     ],
#                     args=['--output_path', output_path]
#                 )
#     """
#     input_name: str
#     """name of the input/output."""

#     then: List[CommandLineElement]
#     """If the input/output specified in name is present, the command-line argument will be replaced at run-time by the expanded value of then."""

#     else_: Optional[List[CommandLineElement]] = None
#     """If the input/output specified in name is not present, the command-line argument will be replaced at run-time by the expanded value of otherwise."""

#     _aliases = {'input_name': 'inputName', 'else_': 'else'}

#     @classmethod
#     def _is_match(cls, string: str) -> bool:
#         try:
#             return 'IfPresent' in json.loads(string)
#         except json.decoder.JSONDecodeError:
#             return False

#     def _to_placeholder_struct(self) -> Dict[str, Any]:
#         then = [
#             maybe_convert_placeholder_to_placeholder_string(item)
#             for item in self.then
#         ] if isinstance(self.then, list) else self.then
#         struct = {'IfPresent': {'InputName': self.input_name, 'Then': then}}
#         if self.else_:
#             otherwise = [
#                 maybe_convert_placeholder_to_placeholder_string(item)
#                 for item in self.else_
#             ] if isinstance(self.else_, list) else self.else_
#             struct['IfPresent']['Else'] = otherwise
#         return struct

#     def _to_placeholder_string(self) -> str:
#         return json.dumps(self._to_placeholder_struct())

#     @classmethod
#     def _from_placeholder_string(
#             cks, placeholder_string: str) -> 'IfPresentPlaceholder':
#         struct = json_load_nested_placeholder_aware(placeholder_string)
#         struct_body = struct['IfPresent']

#         then = struct_body['Then']
#         then = [
#             maybe_convert_placeholder_string_to_placeholder(item)
#             for item in then
#         ] if isinstance(then, list) else then

#         else_ = struct_body.get('Else')
#         else_ = [
#             maybe_convert_placeholder_string_to_placeholder(item)
#             for item in else_
#         ] if isinstance(else_, list) else else_
#         kwargs = {
#             'input_name': struct_body['InputName'],
#             'then': then,
#             'else_': else_
#         }
#         return IfPresentPlaceholder(**kwargs)

#     def _transform_else(self) -> None:
#         """Use None instead of empty list for optional."""
#         self.else_ = None if self.else_ == [] else self.else_

# class CustomizedDecoder(json.JSONDecoder):

#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)

#         def parse_array(*_args, **_kwargs):
#             values, end = JSONArray(*_args, **_kwargs)
#             for i, item in enumerate(values):
#                 if isinstance(item, dict):
#                     values[i] = json.dumps(item)
#             return values, end

#         self.parse_array = parse_array
#         self.scan_once = py_make_scanner(self)

# def json_load_nested_placeholder_aware(
#     placeholder_string: str
# ) -> Union[str, Dict[str, Union[str, List[str], dict]]]:
#     try:
#         return json.loads(placeholder_string, cls=CustomizedDecoder)
#     except json.JSONDecodeError:
#         return placeholder_string

# def maybe_convert_placeholder_string_to_placeholder(
#         placeholder_string: str) -> CommandLineElement:
#     """Infers if a command is a placeholder and converts it to the correct
#     Placeholder object.

#     Args:
#         arg (str): The arg or command to possibly convert.

#     Returns:
#         CommandLineElement: The converted command or original string.
#     """
#     if not placeholder_string.startswith('{'):
#         return placeholder_string

#     # order matters here!
#     from_string_placeholders = [
#         ExecutorInputPlaceholder,
#         IfPresentPlaceholder,
#         ConcatPlaceholder,
#         InputValuePlaceholder,
#         InputPathPlaceholder,
#         InputUriPlaceholder,
#         OutputPathPlaceholder,
#         OutputUriPlaceholder,
#         OutputParameterPlaceholder,
#     ]
#     for placeholder_struct in from_string_placeholders:
#         if placeholder_struct._is_match(placeholder_string):
#             return placeholder_struct._from_placeholder_string(
#                 placeholder_string)
#     return placeholder_string

# def maybe_convert_placeholder_to_placeholder_string(
#         placeholder: CommandLineElement) -> str:
#     """Converts a placeholder to a placeholder string if it's a subclass of
#     Placeholder.

#     Args:
#         placeholder (Placeholder): The placeholder to convert.

#     Returns:
#         str: The placeholder string.
#     """
#     if isinstance(placeholder, Placeholder):
#         return placeholder._to_placeholder_struct() if hasattr(
#             placeholder,
#             '_to_placeholder_struct') else placeholder._to_placeholder_string()
#     return placeholder

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
