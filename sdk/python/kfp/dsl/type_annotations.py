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
from typing import Union, Type, Optional
from kfp.dsl import artifact_types

DEFAULT_ARTIFACT_SCHEMA_VERSION = '0.0.1'


class OutputPath:
    """Type annotation used in component definitions for indicating a parameter
    is a path to an output. The path parameter typed with this annotation can
    be treated as a locally accessible filepath within the component body.

    The argument typed with this annotation is provided at runtime by the executing backend and does not need to be passed as an input by the pipeline author (see example).


    Args:
        type: The type of the value written to the output path.

    Example:
      ::

        @dsl.component
        def create_parameter(
                message: str,
                output_parameter_path: OutputPath(str),
        ):
            with open(output_parameter_path, 'w') as f:
                f.write(message)


        @dsl.component
        def consume_parameter(message: str):
            print(message)


        @dsl.pipeline(name='my-pipeline', pipeline_root='gs://my-bucket')
        def my_pipeline(message: str = 'default message'):
            create_param_op = create_parameter(message=message)
            consume_parameter(message=create_param_op.outputs['output_parameter_path'])
    """

    def __init__(self, type=None):
        self.type = construct_type_for_inputpath_or_outputpath(type)

    def __eq__(self, other):
        return isinstance(other, OutputPath) and self.type == other.type


class InputPath:
    """Type annotation used in component definitions for indicating a parameter
    is a path to an input.

    Example:
      ::

        @dsl.component
        def create_dataset(dataset_path: OutputPath('Dataset'),):
            import json
            dataset = {'my_dataset': [[1, 2, 3], [4, 5, 6]]}
            with open(dataset_path, 'w') as f:
                json.dump(dataset, f)


        @dsl.component
        def consume_dataset(dataset: InputPath('Dataset')):
            print(dataset)


        @dsl.pipeline(name='my-pipeline', pipeline_root='gs://my-bucket')
        def my_pipeline():
            create_dataset_op = create_dataset()
            consume_dataset(dataset=create_dataset_op.outputs['dataset_path'])
    """

    def __init__(self, type=None):
        self.type = construct_type_for_inputpath_or_outputpath(type)

    def __eq__(self, other):
        return isinstance(other, InputPath) and self.type == other.type


def construct_type_for_inputpath_or_outputpath(
        type_: Union[str, Type, None]) -> Union[str, None]:
    if is_artifact_class(type_):
        return create_bundled_artifact_type(type_.schema_title,
                                            type_.schema_version)
    elif isinstance(type_, str) and type_.lower() in ARTIFACT_CLASSES_MAPPING:
        # v1 artifact backward compat, e.g. dsl.OutputPath('Dataset')
        return create_bundled_artifact_type(
            ARTIFACT_CLASSES_MAPPING[type_.lower()].schema_title)
    elif type_utils.get_parameter_type(type_):
        return type_
    else:
        # v1 unknown type dsl.OutputPath('MyCustomType')
        return create_bundled_artifact_type(
            artifact_types.Artifact.schema_title)


def is_artifact_class(artifact_class_or_instance: Type) -> bool:
    # we do not yet support non-pre-registered custom artifact types with instance_schema attribute
    return hasattr(artifact_class_or_instance, 'schema_title') and hasattr(
        artifact_class_or_instance, 'schema_version')


def create_bundled_artifact_type(schema_title: str,
                                 schema_version: Optional[str] = None) -> str:
    if not isinstance(schema_title, str):
        raise ValueError
    return schema_title + '@' + (
        schema_version or DEFAULT_ARTIFACT_SCHEMA_VERSION)


ARTIFACT_CLASSES_MAPPING = {
    'artifact': artifact_types.Artifact,
    'model': artifact_types.Model,
    'dataset': artifact_types.Dataset,
    'metrics': artifact_types.Metrics,
    'classificationmetrics': artifact_types.ClassificationMetrics,
    'slicedclassificationmetrics': artifact_types.SlicedClassificationMetrics,
    'html': artifact_types.HTML,
    'markdown': artifact_types.Markdown,
}