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
# import os

from google.cloud.aiplatform.metadata.schema.google import artifact_schema
from google.cloud.aiplatform.metadata.schema.google.artifact_schema import \
    VertexModel
from kfp import compiler
from kfp import dsl
from kfp.dsl import Input
from kfp.dsl import Output

PACKAGES_TO_INSTALL = ['google-cloud-aiplatform']


@dsl.component(packages_to_install=PACKAGES_TO_INSTALL)
def model_producer(model: Output[artifact_schema.VertexModel]):
    with open(model.path, 'w') as f:
        f.write('my model')


@dsl.component(packages_to_install=PACKAGES_TO_INSTALL)
def model_consumer(model: Input[VertexModel]):
    print('artifact.type: ', type(model))
    print('artifact.name: ', model.name)
    print('artifact.uri: ', model.uri)
    print('artifact.metadata: ', model.metadata)


@dsl.pipeline(name='pipeline-with-gcpc-types')
def my_pipeline():
    producer_task = model_producer()
    model_consumer(model=producer_task.outputs['model'])


if __name__ == '__main__':
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
