# Copyright 2022 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import collections
import dataclasses
import inspect
from collections import abc
from typing import (Any, Dict, Iterable, Mapping, MutableMapping,
                    MutableSequence, Optional, OrderedDict, Sequence, Tuple,
                    Type, TypeVar, Union)

# TODO: implement in structures
# TODO: accumulate errors on typecasting
# TODO: support for future annotations

T = TypeVar('T')

PRIMITIVE_TYPES = {int, str, float, bool}

# typing.Optional.__origin__ is typing.Union
UNION_TYPES = {Union}

# do not need typing.Dict, because __origin__ is list
ITERABLE_TYPES = {
    list,
    abc.Sequence,
    abc.MutableSequence,
    Sequence,
    MutableSequence,
    Iterable,
}

# do not need typing.Dict, because __origin__ is list
MAPPING_TYPES = {
    dict, abc.Mapping, abc.MutableMapping, Mapping, MutableMapping, OrderedDict,
    collections.OrderedDict
}

OTHER_SUPPORTED_TYPES = {type(None), Any}
SUPPORTED_TYPES = PRIMITIVE_TYPES | UNION_TYPES | ITERABLE_TYPES | MAPPING_TYPES | OTHER_SUPPORTED_TYPES


def _snake_case_to_camel_case(string: str) -> str:
    """Converts snake_case to camelCase by removing underscores and converting
    subsequent letter to uppercase."""
    if '_' not in string:
        return string
    prev_space = False
    chars = []
    for char in string:
        if char == '_':
            prev_space = True
            continue
        elif prev_space:
            chars.append(char.upper())
        else:
            chars.append(char.lower())
        prev_space = char == '_'
    return ''.join(chars)


class BaseModel:
    """BaseModel for structures.

    Subclasses are dataclasses with methods to support for converting to
    and from dict, type enforcement, and user-defined validation logic.
    """

    def __init_subclass__(self) -> None:
        class_instance = dataclasses.dataclass(self)
        for field in dataclasses.fields(class_instance):
            self._recursively_validate_type_is_supported(field.type)

    def to_dict(self) -> Dict[str, Any]:
        """Recursively convert to dictionary."""
        return convert_object_to_dict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        """Recursively load from dictionary."""
        return _load_basemodel_helper(cls, data)

    @property
    def types(self) -> Dict[str, type]:
        """Dictionary of field name-type pairs."""
        return {field.name: field.type for field in dataclasses.fields(self)}

    @classmethod
    @property
    def fields(cls) -> Tuple[dataclasses.Field, ...]:
        """The fields on this object."""
        return dataclasses.fields(cls)

    @classmethod
    def _recursively_validate_type_is_supported(cls, type_: type) -> None:
        """Walks the type (generic and subtypes) and checks if it is supported.

        Args:
            type_ (type): Type to check.

        Raises:
            TypeError: If type is unsupported.
        """
        if type_ in SUPPORTED_TYPES or _is_basemodel(type_):
            return

        if _get_origin_py37(type_) not in SUPPORTED_TYPES:
            raise TypeError(
                f'Type {type_} is not a supported type fields on child class of {BaseModel.__name__}: {cls.__name__}.'
            )

        args = _get_args_py37(type_) or [Any, Any]
        for arg in args:
            cls._recursively_validate_type_is_supported(arg)


def convert_object_to_dict(obj: Any) -> Dict[str, Any]:
    """Converts an object to structure (usually a dict).

    Serializes all properties that do not start with underscores. If the
    type of some property is a class that has .to_dict class method,
    that method is used for conversion. Used by the ModelBase class.
    """
    signature = inspect.signature(obj.__init__)  #Needed for default values

    result = {}
    for snake_case_name in signature.parameters:  #TODO: Make it possible to specify the field ordering regardless of the presence of default values
        camel_case_name = _snake_case_to_camel_case(snake_case_name)
        value = getattr(obj, snake_case_name)
        if snake_case_name.startswith('_'):
            continue
        if hasattr(value, 'to_dict'):
            result[camel_case_name] = value.to_dict()
        elif isinstance(value, list):
            result[camel_case_name] = [
                (x.to_dict() if hasattr(x, 'to_dict') else x) for x in value
            ]
        elif isinstance(value, dict):
            result[camel_case_name] = {
                k: (v.to_dict() if hasattr(v, 'to_dict') else v)
                for k, v in value.items()
            }
        else:
            param = signature.parameters.get(snake_case_name, None)
            if param is None or param.default == inspect.Parameter.empty or value != param.default:
                result[camel_case_name] = value

    return result


def _is_basemodel(obj: Any) -> bool:
    """Checks if object is a subclass of BaseModel.

    Args:
        obj (Any): Any object

    Returns:
        bool: Is a subclass of BaseModel.
    """
    return inspect.isclass(obj) and issubclass(obj, BaseModel)


def _get_origin_py37(type_: Type) -> Optional[Type]:
    """typing.get_origin is introduced in Python 3.8, but we need a get_origin
    that is compatible with 3.7.

    Args:
        type_ (Type): A type.

    Returns:
        Type: The origin of `type_`.
    """
    # uses typing for types
    return type_.__origin__ if hasattr(type_, '__origin__') else None


def _get_args_py37(type_: Type) -> Tuple[Type]:
    """typing.get_args is introduced in Python 3.8, but we need a get_args that
    is compatible with 3.7.

    Args:
        type_ (Type): A type.

    Returns:
        Tuple[Type]: The type arguments of `type_`.
    """
    # uses typing for types
    return type_.__args__ if hasattr(type_, '__args__') else tuple()


def _load_basemodel_helper(type_: Any, data: Any) -> Any:
    """Helper function for recursively loading a BaseModel.

    Args:
        type_ (Any): The type of the object to load. Typically an instance of `type`, `BaseModel` or `Any`.
        data (Any): The data to load.

    Returns:
        T: The loaded object.
    """
    if isinstance(type_, str):
        raise TypeError(
            'Please do not use built-in collection types as generics (e.g., list[int]) and do not include the import line `from __future__ import annotations`. Please use the corresponding generic from typing (e.g., List[int]).'
        )

    # catch unsupported types early
    type_or_generic = _get_origin_py37(type_) or type_
    if type_or_generic not in SUPPORTED_TYPES and not _is_basemodel(type_):
        raise TypeError(
            f'Unsupported type: {type_}. Cannot load data into object.')

    # if don't have any type information, return data as is
    if type_ is Any:
        return data

    # if type is NoneType and data is None, return data/None
    if type_ is type(None):
        if data is None:
            return data
        else:
            raise TypeError(
                f'Expected value None for type NoneType. Got: {data}')

    # handle primitives, with typecasting
    if type_ in PRIMITIVE_TYPES:
        return type_(data)

    # simple types are handled, now handle for container types
    origin = _get_origin_py37(type_)
    args = _get_args_py37(type_) or [
        Any, Any
    ]  # if there is an inner type in the generic, use it, else use Any
    # recursively load iterable objects
    if origin in ITERABLE_TYPES:
        for arg in args:  # TODO handle errors
            return [_load_basemodel_helper(arg, element) for element in data]

    # recursively load mapping objects
    if origin in MAPPING_TYPES:
        if len(args) != 2:
            raise TypeError(
                f'Expected exactly 2 type arguments for mapping type {type_}.')
        return {
            _load_basemodel_helper(args[0], k):
            _load_basemodel_helper(args[1], v)  # type: ignore
            for k, v in data.items()
        }

    # if the type is a Union, try to load the data into each of the types,
    # greedily accepting the first annotation arg that works --> the developer
    # can indicate which types are preferred based on the annotation arg order
    if origin in UNION_TYPES:
        # don't try to cast none if the union type is optional
        if type(None) in args and data is None:
            return None
        for arg in args:
            return _load_basemodel_helper(args[0], data)

    # finally, handle the cases where the type is an instance of baseclass
    if _is_basemodel(type_):
        fields = dataclasses.fields(type_)
        basemodel_kwargs = {}
        for field in fields:
            snake_key = field.name
            camel_key = _snake_case_to_camel_case(snake_key)
            value = data.get(camel_key)
            if value is None:
                if field.default is dataclasses.MISSING and field.default_factory is dataclasses.MISSING:
                    raise ValueError(f'Missing required field: {camel_key}')
                value = field.default if field.default is not dataclasses.MISSING else field.default_factory(
                )
            else:
                value = _load_basemodel_helper(field.type, value)
            basemodel_kwargs[snake_key] = value
        return type_(**basemodel_kwargs)

    raise TypeError(
        f'Unknown error when loading data: {data} into type {type_}')
