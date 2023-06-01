# Copyright 2021 The Kubeflow Authors
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
"""Classes for input/output type annotations in KFP SDK.

These are only compatible with v2 Pipelines.
"""

import re
from typing import List, Type, TypeVar, Union

from kfp.dsl import artifact_types
from kfp.dsl import type_annotations as dsl_type_annotations


class InputAnnotation:
    """Marker type for input artifacts."""


class OutputAnnotation:
    """Marker type for output artifacts."""


def is_Input_Output_artifact_annotation(typ) -> bool:
    if not hasattr(typ, '__metadata__'):
        return False

    if typ.__metadata__[0] not in [InputAnnotation, OutputAnnotation]:
        return False

    return True


def is_input_artifact(typ) -> bool:
    """Returns True if typ is of type Input[T]."""
    if not is_Input_Output_artifact_annotation(typ):
        return False

    return typ.__metadata__[0] == InputAnnotation


def is_output_artifact(typ) -> bool:
    """Returns True if typ is of type Output[T]."""
    if not is_Input_Output_artifact_annotation(typ):
        return False

    return typ.__metadata__[0] == OutputAnnotation


def get_io_artifact_class(typ):
    from kfp.dsl import Input
    from kfp.dsl import Output
    if not is_Input_Output_artifact_annotation(typ):
        return None
    if typ == Input or typ == Output:
        return None

    # extract inner type from list of artifacts
    inner = typ.__args__[0]
    if hasattr(inner, '__origin__') and inner.__origin__ == list:
        return inner.__args__[0]

    return inner


def get_io_artifact_annotation(typ):
    if not is_Input_Output_artifact_annotation(typ):
        return None

    return typ.__metadata__[0]


T = TypeVar('T')


def maybe_strip_optional_from_annotation(annotation: T) -> T:
    """Strips 'Optional' from 'Optional[<type>]' if applicable.

    For example::
      Optional[str] -> str
      str -> str
      List[int] -> List[int]

    Args:
      annotation: The original type annotation which may or may not has
        `Optional`.

    Returns:
      The type inside Optional[] if Optional exists, otherwise the original type.
    """
    if getattr(annotation, '__origin__',
               None) is Union and annotation.__args__[1] is type(None):
        return annotation.__args__[0]
    return annotation


def maybe_strip_optional_from_annotation_string(annotation: str) -> str:
    if annotation.startswith('Optional[') and annotation.endswith(']'):
        return annotation.lstrip('Optional[').rstrip(']')
    return annotation


def get_short_type_name(type_name: str) -> str:
    """Extracts the short form type name.

    This method is used for looking up serializer for a given type.

    For example::
      typing.List -> List
      typing.List[int] -> List
      typing.Dict[str, str] -> Dict
      List -> List
      str -> str

    Args:
      type_name: The original type name.

    Returns:
      The short form type name or the original name if pattern doesn't match.
    """
    match = re.match('(typing\.)?(?P<type>\w+)(?:\[.+\])?', type_name)
    return match['type'] if match else type_name


def is_list_of_artifacts(
    type_var: Union[Type[List[artifact_types.Artifact]],
                    Type[artifact_types.Artifact]]
) -> bool:
    # the type annotation for this function's `type_var` parameter may not actually be a subclass of the KFP SDK's Artifact class for custom artifact types
    return getattr(type_var, '__origin__',
                   None) == list and dsl_type_annotations.is_artifact_class(
                       type_var.__args__[0])
