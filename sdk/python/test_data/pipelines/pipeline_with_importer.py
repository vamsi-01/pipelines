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
"""Pipeline using dsl.importer."""

from typing import NamedTuple

from kfp import compiler
from kfp import dsl
from kfp.dsl import component
from kfp.dsl import Dataset
from kfp.dsl import importer
from kfp.dsl import Input
from kfp.dsl import Model


@component
def train(
    dataset: Input[Dataset]
) -> NamedTuple('Outputs', [
    ('scalar', str),
    ('model', Model),
]):
    """Dummy Training step."""
    with open(dataset.path) as f:
        data = f.read()
    print('Dataset:', data)

    scalar = '123'
    model = f'My model trained using data: {data}'

    from collections import namedtuple
    output = namedtuple('Outputs', ['scalar', 'model'])
    return output(scalar, model)


@component
def pass_through_op(value: str) -> str:
    return value


@dsl.pipeline(name='pipeline-with-importer')
def my_pipeline(dataset2: str = 'gs://ml-pipeline-playground/shakespeare2.txt'):

    importer1 = importer(
        artifact_uri='gs://ml-pipeline-playground/shakespeare1.txt',
        artifact_class=Dataset,
        reimport=False,
        metadata={'key': 'value'})
    train1 = train(dataset=importer1.output)

    with dsl.Condition(train1.outputs['scalar'] == '123'):
        importer2 = importer(
            artifact_uri=dataset2, artifact_class=Dataset, reimport=True)
        train(dataset=importer2.output)


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform
    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
    pipeline_name = __file__.split('/')[-1].replace('_', '-').replace('.py', '')
    display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
    job_id = f'{pipeline_name}-{display_name}'
    aiplatform.PipelineJob(
        template_path=ir_file,
        pipeline_root='gs://cjmccarthy-kfp-default-bucket',
        display_name=pipeline_name,
        job_id=job_id).submit()
    url = f'https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/{pipeline_name}-{display_name}?project=271009669852'
    webbrowser.open_new_tab(url)
