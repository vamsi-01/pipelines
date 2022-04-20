# Copyright 2021-2022 The Kubeflow Authors
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
"""Definitions for component spec."""

import dataclasses
import itertools
import json
from typing import (Any, Dict, List, Mapping, Optional, Sequence, Type,
                    TypeVar, Union)

import pydantic
import yaml
from kfp.components import utils
from kfp.components import v1_components
from kfp.components import v1_structures
from kfp.utils import ir_utils

T = TypeVar("T")


def _snake_case_to_camel_case(string: str) -> str:
    if "_" not in string:
        return string

    prev_space = False
    chars = []
    for char in chars:
        if char == "_":
            prev_space = True
            continue
        elif prev_space:
            chars.append(char.upper())
        else:
            chars.append(char.lower())
        prev_space = False
    return "".join(chars)


class BaseModel:

    def __init_subclass__(self: T) -> T:
        return dataclasses.dataclass(self)

    def __post_init__(self):
        self._recurisvely_validate_types()
        if hasattr(self, "validate") and callable(self.validate):
            self.validate()

    def to_dict(self) -> Dict[str, Any]:
        dictionary = {}
        for field in self.fields:
            field = _snake_case_to_camel_case(field)
            value = getattr(self, field)
            dictionary[field] = value.to_dict() if isinstance(
                value, BaseModel) else value
        return dictionary

    def __dict__(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @property
    def types(self) -> Dict[str, type]:
        return {field.name: field.type for field in dataclasses.fields(self)}

    @property
    def fields(self) -> List[str]:
        return [field.name for field in dataclasses.fields(self)]

    def _recurisvely_validate_types(self) -> None:
        pass
        for field in self.fields:
            value = getattr(self, field)
            type_ann = self.types[field]
            if not isinstance(value, type_ann):
                raise TypeError(
                    f"{field} is not of type {self.types[field]}: {value}")


class InputSpec(BaseModel):
    """Component input definitions.

    Attributes:
        type: The type of the input.
        default: Optional; the default value for the input.
        description: Optional: the user description of the input.
        optional: Wether the input is optional. An input is optional when it has
            an explicit default value.
    """
    type: Union[str, dict]
    default: Optional[Any] = None
    description: Optional[str] = None

    def __init__(self, **data):
        super().__init__(**data)
        # An input is optional if a default value is explicitly specified.
        self._optional = 'default' in data

    @property
    def optional(self) -> bool:
        return self._optional


class OutputSpec(BaseModel):
    """Component output definitions.

    Attributes:
        type: The type of the output.
        description: Optional: the user description of the output.
    """
    type: Union[str, dict]
    description: Optional[str] = None


class BasePlaceholder(BaseModel):
    """Base class for placeholders that could appear in container cmd and
    args."""
    pass


class InputValuePlaceholder(BasePlaceholder):
    """Class that holds input value for conditional cases.

    Attributes:
        input_name: name of the input.
    """
    input_name: str


class InputPathPlaceholder(BasePlaceholder):
    """Class that holds input path for conditional cases.

    Attributes:
        input_name: name of the input.
    """
    input_name: str


class InputUriPlaceholder(BasePlaceholder):
    """Class that holds input uri for conditional cases.

    Attributes:
        input_name: name of the input.
    """
    # TODO: input_name: str = pydantic.Field(alias='inputUri')
    input_name: str


class OutputPathPlaceholder(BasePlaceholder):
    """Class that holds output path for conditional cases.

    Attributes:
        output_name: name of the output.
    """
    # TODO: output_name: str = pydantic.Field(alias='outputPath')
    output_name: str


class OutputUriPlaceholder(BasePlaceholder):
    """Class that holds output uri for conditional cases.

    Attributes:
        output_name: name of the output.
    """
    # TODO: output_name: str = pydantic.Field(alias='outputUri')
    output_name: str


ValidCommandArgs = Union[str, InputValuePlaceholder, InputPathPlaceholder,
                         InputUriPlaceholder, OutputPathPlaceholder,
                         OutputUriPlaceholder, 'IfPresentPlaceholder',
                         'ConcatPlaceholder']


class ConcatPlaceholder(BasePlaceholder):
    """Class that extends basePlaceholders for concatenation.

    Attributes:
        items: string or ValidCommandArgs for concatenation.
    """
    # TODO: items: Sequence[ValidCommandArgs] = pydantic.Field(alias='concat')
    items: Sequence[ValidCommandArgs]


class IfPresentPlaceholderStructure(BaseModel):
    """Class that holds structure for conditional cases.

    Attributes:
        input_name: name of the input/output.
        then: If the input/output specified in name is present,
            the command-line argument will be replaced at run-time by the
            expanded value of then.
        otherwise: If the input/output specified in name is not present,
            the command-line argument will be replaced at run-time by the
            expanded value of otherwise.
    """
    input_name: str
    then: Sequence[ValidCommandArgs]
    # TODO: otherwise: Optional[Sequence[ValidCommandArgs]] = pydantic.Field(
    #     None, alias='else')
    otherwise: Optional[Sequence[ValidCommandArgs]]

    # TODO
    # @pydantic.validator('otherwise', allow_reuse=True)
    # def empty_otherwise_sequence(cls, v):
    #     if v == []:
    #         return None
    #     return v


class IfPresentPlaceholder(BasePlaceholder):
    """Class that extends basePlaceholders for conditional cases.

    Attributes:
        if_present (ifPresent): holds structure for conditional cases.
    """
    # TODO:
    if_structure: IfPresentPlaceholderStructure


# IfPresentPlaceholderStructure.update_forward_refs()
# IfPresentPlaceholder.update_forward_refs()
# ConcatPlaceholder.update_forward_refs()


@dataclasses.dataclass
class ResourceSpec:
    """The resource requirements of a container execution.

    Attributes:
        cpu_limit: Optional; the limit of the number of vCPU cores.
        memory_limit: Optional; the memory limit in GB.
        accelerator_type: Optional; the type of accelerators attached to the
            container.
        accelerator_count: Optional; the number of accelerators attached.
    """
    cpu_limit: Optional[float] = None
    memory_limit: Optional[float] = None
    accelerator_type: Optional[str] = None
    accelerator_count: Optional[int] = None


class ContainerSpec(BaseModel):
    """Container implementation definition.

    Attributes:
        image: The container image.
        command: Optional; the container entrypoint.
        args: Optional; the arguments to the container entrypoint.
        env: Optional; the environment variables to be passed to the container.
        resources: Optional; the specification on the resource requirements.
    """
    image: str
    command: Optional[Sequence[ValidCommandArgs]] = None
    args: Optional[Sequence[ValidCommandArgs]] = None
    env: Optional[Mapping[str, ValidCommandArgs]] = None
    resources: Optional[ResourceSpec] = None

    # @pydantic.validator('command', 'args', allow_reuse=True)
    # def empty_sequence(cls, v):
    #     if v == []:
    #         return None
    #     return v

    # @pydantic.validator('env', allow_reuse=True)
    # def empty_map(cls, v):
    #     if v == {}:
    #         return None
    #     return v


class TaskSpec(BaseModel):
    """The spec of a pipeline task.

    Attributes:
        name: The name of the task.
        inputs: The sources of task inputs. Constant values or PipelineParams.
        dependent_tasks: The list of upstream tasks.
        component_ref: The name of a component spec this task is based on.
        trigger_condition: Optional; an expression which will be evaluated into
            a boolean value. True to trigger the task to run.
        trigger_strategy: Optional; when the task will be ready to be triggered.
            Valid values include: "TRIGGER_STRATEGY_UNSPECIFIED",
            "ALL_UPSTREAM_TASKS_SUCCEEDED", and "ALL_UPSTREAM_TASKS_COMPLETED".
        iterator_items: Optional; the items to iterate on. A constant value or
            a PipelineParam.
        iterator_item_input: Optional; the name of the input which has the item
            from the [items][] collection.
        enable_caching: Optional; whether or not to enable caching for the task.
            Default is True.
        display_name: Optional; the display name of the task. If not specified,
            the task name will be used as the display name.
    """
    name: str
    inputs: Mapping[str, Any]
    dependent_tasks: Sequence[str]
    component_ref: str
    trigger_condition: Optional[str] = None
    trigger_strategy: Optional[str] = None
    iterator_items: Optional[Any] = None
    iterator_item_input: Optional[str] = None
    enable_caching: bool = True
    display_name: Optional[str] = None


class DagSpec(BaseModel):
    """DAG(graph) implementation definition.

    Attributes:
        tasks: The tasks inside the DAG.
        outputs: Defines how the outputs of the dag are linked to the sub tasks.
    """
    tasks: Mapping[str, TaskSpec]
    # TODO(chensun): revisit if we need a DagOutputsSpec class.
    outputs: Mapping[str, Any]


class ImporterSpec(BaseModel):
    """ImporterSpec definition.

    Attributes:
        artifact_uri: The URI of the artifact.
        type_schema: The type of the artifact.
        reimport: Whether or not import an artifact regardless it has been
         imported before.
        metadata: Optional; the properties of the artifact.
    """
    artifact_uri: str
    type_schema: str
    reimport: bool
    metadata: Optional[Mapping[str, Any]] = None


class Implementation(BaseModel):
    """Implementation definition.

    Attributes:
        container: container implementation details.
        graph: graph implementation details.
        importer: importer implementation details.
    """
    container: Optional[ContainerSpec] = None
    graph: Optional[DagSpec] = None
    importer: Optional[ImporterSpec] = None


class ComponentSpec(BaseModel):
    """The definition of a component.

    Attributes:
        name: The name of the component.
        description: Optional; the description of the component.
        inputs: Optional; the input definitions of the component.
        outputs: Optional; the output definitions of the component.
        implementation: The implementation of the component. Either an executor
            (container, importer) or a DAG consists of other components.
    """
    name: str
    implementation: Implementation
    description: Optional[str] = None
    inputs: Optional[Dict[str, InputSpec]] = None
    outputs: Optional[Dict[str, OutputSpec]] = None

    # @pydantic.validator('inputs', 'outputs', allow_reuse=True)
    # def empty_map(cls, v):
    #     if v == {}:
    #         return None
    #     return v

    # @pydantic.root_validator(allow_reuse=True)
    # def validate_placeholders(cls, values):
    #     if values.get('implementation').container is None:
    #         return values
    #     containerSpec: ContainerSpec = values.get('implementation').container

    #     try:
    #         valid_inputs = values.get('inputs').keys()
    #     except AttributeError:
    #         valid_inputs = []

    #     try:
    #         valid_outputs = values.get('outputs').keys()
    #     except AttributeError:
    #         valid_outputs = []

    #     for arg in itertools.chain((containerSpec.command or []),
    #                                (containerSpec.args or [])):
    #         cls._check_valid_placeholder_reference(valid_inputs, valid_outputs,
    #                                                arg)

    #     return values

    @classmethod
    def _check_valid_placeholder_reference(cls, valid_inputs: Sequence[str],
                                           valid_outputs: Sequence[str],
                                           arg: ValidCommandArgs) -> None:
        """Validates placeholder reference existing input/output names.

        Args:
            valid_inputs: The existing input names.
            valid_outputs: The existing output names.
            arg: The placeholder argument for checking.

        Raises:
            ValueError: if any placeholder references a non-existing input or
                output.
            TypeError: if any argument is neither a str nor a placeholder
                instance.
        """

        if isinstance(
                arg,
            (InputValuePlaceholder, InputPathPlaceholder, InputUriPlaceholder)):
            if arg.input_name not in valid_inputs:
                raise ValueError(
                    f'Argument "{arg}" references non-existing input.')
        elif isinstance(arg, (OutputPathPlaceholder, OutputUriPlaceholder)):
            if arg.output_name not in valid_outputs:
                raise ValueError(
                    f'Argument "{arg}" references non-existing output.')
        elif isinstance(arg, IfPresentPlaceholder):
            if arg.if_structure.input_name not in valid_inputs:
                raise ValueError(
                    f'Argument "{arg}" references non-existing input.')
            for placeholder in itertools.chain(arg.if_structure.then or [],
                                               arg.if_structure.otherwise or
                                               []):
                cls._check_valid_placeholder_reference(valid_inputs,
                                                       valid_outputs,
                                                       placeholder)
        elif isinstance(arg, ConcatPlaceholder):
            for placeholder in arg.items:
                cls._check_valid_placeholder_reference(valid_inputs,
                                                       valid_outputs,
                                                       placeholder)
        elif not isinstance(arg, str):
            raise TypeError(f'Unexpected argument "{arg}".')

    @classmethod
    def from_v1_component_spec(
            cls,
            v1_component_spec: v1_structures.ComponentSpec) -> 'ComponentSpec':
        """Converts V1 ComponentSpec to V2 ComponentSpec.

        Args:
            v1_component_spec: The V1 ComponentSpec.

        Returns:
            Component spec in the form of V2 ComponentSpec.

        Raises:
            ValueError: If implementation is not found.
            TypeError: if any argument is neither a str nor Dict.
        """
        component_dict = v1_component_spec.to_dict()
        if component_dict.get('implementation') is None:
            raise ValueError('Implementation field not found')
        if 'container' not in component_dict.get('implementation'):
            raise NotImplementedError

        def _transform_arg(arg: Union[str, Dict[str, str]]) -> ValidCommandArgs:
            if isinstance(arg, str):
                return arg
            if 'inputValue' in arg:
                return InputValuePlaceholder(
                    input_name=utils.sanitize_input_name(arg['inputValue']))
            if 'inputPath' in arg:
                return InputPathPlaceholder(
                    input_name=utils.sanitize_input_name(arg['inputPath']))
            if 'inputUri' in arg:
                return InputUriPlaceholder(
                    input_name=utils.sanitize_input_name(arg['inputUri']))
            if 'outputPath' in arg:
                return OutputPathPlaceholder(
                    output_name=utils.sanitize_input_name(arg['outputPath']))
            if 'outputUri' in arg:
                return OutputUriPlaceholder(
                    output_name=utils.sanitize_input_name(arg['outputUri']))
            if 'if' in arg:
                if_placeholder_values = arg['if']
                if_placeholder_values_then = list(if_placeholder_values['then'])
                try:
                    if_placeholder_values_else = list(
                        if_placeholder_values['else'])
                except KeyError:
                    if_placeholder_values_else = []

                IfPresentPlaceholderStructure.update_forward_refs()
                return IfPresentPlaceholder(
                    if_structure=IfPresentPlaceholderStructure(
                        input_name=utils.sanitize_input_name(
                            if_placeholder_values['cond']['isPresent']),
                        then=list(
                            _transform_arg(val)
                            for val in if_placeholder_values_then),
                        otherwise=list(
                            _transform_arg(val)
                            for val in if_placeholder_values_else)))
            if 'concat' in arg:
                ConcatPlaceholder.update_forward_refs()

                return ConcatPlaceholder(
                    concat=list(_transform_arg(val) for val in arg['concat']))
            raise ValueError(
                f'Unexpected command/argument type: "{arg}" of type "{type(arg)}".'
            )

        implementation = component_dict['implementation']['container']
        implementation['command'] = [
            _transform_arg(command)
            for command in implementation.pop('command', [])
        ]
        implementation['args'] = [
            _transform_arg(command)
            for command in implementation.pop('args', [])
        ]
        implementation['env'] = {
            key: _transform_arg(command)
            for key, command in implementation.pop('env', {}).items()
        }

        container_spec = ContainerSpec(image=implementation['image'])

        # Workaround for https://github.com/samuelcolvin/pydantic/issues/2079
        def _copy_model(obj):
            if isinstance(obj, BaseModel):
                return obj.copy(deep=True)
            return obj

        # Must assign these after the constructor call, otherwise it won't work.
        if implementation['command']:
            container_spec.command = [
                _copy_model(cmd) for cmd in implementation['command']
            ]
        if implementation['args']:
            container_spec.args = [
                _copy_model(arg) for arg in implementation['args']
            ]
        if implementation['env']:
            container_spec.env = {
                k: _copy_model(v) for k, v in implementation['env']
            }

        return ComponentSpec(
            name=component_dict.get('name', 'name'),
            description=component_dict.get('description'),
            implementation=Implementation(container=container_spec),
            inputs={
                utils.sanitize_input_name(spec['name']): InputSpec(
                    type=spec.get('type', 'Artifact'),
                    default=spec.get('default', None))
                for spec in component_dict.get('inputs', [])
            },
            outputs={
                utils.sanitize_input_name(spec['name']):
                OutputSpec(type=spec.get('type', 'Artifact'))
                for spec in component_dict.get('outputs', [])
            })

    def to_v1_component_spec(self) -> v1_structures.ComponentSpec:
        """Converts to v1 ComponentSpec.

        Returns:
            Component spec in the form of V1 ComponentSpec.

        Needed until downstream accept new ComponentSpec.
        """

        def _transform_arg(arg: ValidCommandArgs) -> Any:
            if isinstance(arg, str):
                return arg
            if isinstance(arg, InputValuePlaceholder):
                return v1_structures.InputValuePlaceholder(arg.input_name)
            if isinstance(arg, InputPathPlaceholder):
                return v1_structures.InputPathPlaceholder(arg.input_name)
            if isinstance(arg, InputUriPlaceholder):
                return v1_structures.InputUriPlaceholder(arg.input_name)
            if isinstance(arg, OutputPathPlaceholder):
                return v1_structures.OutputPathPlaceholder(arg.output_name)
            if isinstance(arg, OutputUriPlaceholder):
                return v1_structures.OutputUriPlaceholder(arg.output_name)
            if isinstance(arg, IfPresentPlaceholder):
                return v1_structures.IfPlaceholder(arg.if_structure)
            if isinstance(arg, ConcatPlaceholder):
                return v1_structures.ConcatPlaceholder(arg.concat)
            raise ValueError(
                f'Unexpected command/argument type: "{arg}" of type "{type(arg)}".'
            )

        return v1_structures.ComponentSpec(
            name=self.name,
            inputs=[
                v1_structures.InputSpec(
                    name=name,
                    type=input_spec.type,
                    default=input_spec.default,
                ) for name, input_spec in self.inputs.items()
            ],
            outputs=[
                v1_structures.OutputSpec(
                    name=name,
                    type=output_spec.type,
                ) for name, output_spec in self.outputs.items()
            ],
            implementation=v1_structures.ContainerImplementation(
                container=v1_structures.ContainerSpec(
                    image=self.implementation.container.image,
                    command=[
                        _transform_arg(cmd)
                        for cmd in self.implementation.container.command or []
                    ],
                    args=[
                        _transform_arg(arg)
                        for arg in self.implementation.container.args or []
                    ],
                    env={
                        name: _transform_arg(value) for name, value in
                        self.implementation.container.env or {}
                    },
                )),
        )

    @classmethod
    def load_from_component_yaml(cls, component_yaml: str) -> 'ComponentSpec':
        """Loads V1 or V2 component yaml into ComponentSpec.

        Args:
            component_yaml: the component yaml in string format.

        Returns:
            Component spec in the form of V2 ComponentSpec.
        """

        json_component = yaml.safe_load(component_yaml)
        try:
            return ComponentSpec.parse_obj(json_component)
        except (pydantic.ValidationError, AttributeError):
            v1_component = v1_components._load_component_spec_from_component_text(
                component_yaml)
            return cls.from_v1_component_spec(v1_component)

    def save_to_component_yaml(self, output_file: str) -> None:
        """Saves ComponentSpec into yaml file.

        Args:
            output_file: File path to store the component yaml.
        """
        json_text = json.dumps(self.dict(), sort_keys=True)
        ir_utils._write_ir_to_file(json_text, output_file)
