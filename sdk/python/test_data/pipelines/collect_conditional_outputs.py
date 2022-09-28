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

from kfp import dsl


@dsl.component
def process_positive(num: int) -> int:
    return num


@dsl.component
def process_negative(num: int) -> int:
    return num


@dsl.component
def double(num: int) -> int:
    return 2 * num


@dsl.pipeline
def inner(num: int) -> int:
    with dsl.Condition(num > 0, name='positive-condition'):
        pos_result = process_positive(num=num)

    with dsl.Condition(num < 0, name='negative-condition'):
        neg_result = process_negative(num=num)

    return pos_result.output


@dsl.pipeline
def my_pipeline(num: int):

    res = inner(num=num)

    double(num=res.output)


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    # compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
    pipeline_name = __file__.split('/')[-1].replace('_', '-').replace('.py', '')
    display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
    job_id = f'{pipeline_name}-{display_name}'
    aiplatform.PipelineJob(
        parameter_values={
            'num': 3
        },
        template_path=ir_file,
        pipeline_root='gs://cjmccarthy-kfp-default-bucket',
        display_name=pipeline_name,
        job_id=job_id).submit()
    url = f'https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/{pipeline_name}-{display_name}?project=271009669852'
    webbrowser.open_new_tab(url)
