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

from typing import List

from kfp import dsl


@dsl.component
def identity(string: str) -> str:
    print(string)
    return string


@dsl.component
def contains(string: str, items: List[str]) -> bool:
    return string in items


@dsl.pipeline
def my_pipeline():
    outputs = []
    with dsl.ParallelFor(['a', 'b']) as item:
        result = identity(string=item)
        outputs.append(result.output)
    contains(string='a', items=outputs)


# if __name__ == '__main__':
#     compiler.Compiler().compile(
#         pipeline_func=my_pipeline,
#         package_path=__file__.replace('.py', '.yaml'))

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
        template_path=ir_file,
        pipeline_root='gs://cjmccarthy-kfp-default-bucket',
        display_name=pipeline_name,
        job_id=job_id).submit()
    url = f'https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/{pipeline_name}-{display_name}?project=271009669852'
    webbrowser.open_new_tab(url)
