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

from typing import Optional, List, Dict

from kfp import compiler, dsl
from kfp.dsl import component


@component
def print_op(msg: str, msg2: Optional[str] = None):
    print(f'msg: {msg}, msg2: {msg2}')


@component
def print_bool(boolean: bool):
    print(type(boolean))


@dsl.pipeline(name='pipeline-with-nested-loops')
def my_pipeline(loop_parameter: List[Dict[str, bool]] = [
    {
        "p_a": [{
            "a": True
        }, {
            "a": True
        }],
        "p_b": "hello",
    },
    {
        "p_a": [{
            "a": True
        }, {
            "a": True
        }],
        "p_b": "halo",
    },
]):
    # Nested loop with withParams loop args
    with dsl.ParallelFor(loop_parameter) as item:
        print_op(msg=item.p_b)
        with dsl.ParallelFor(item.p_a) as item_p_a:
            print_bool(boolean=item_p_a.a)

    # # Nested loop with withItems loop args
    # with dsl.ParallelFor(['1', '2']) as outter_item:
    #     print_op(msg=outter_item)
    #     with dsl.ParallelFor(['100', '200', '300']) as inner_item:
    #         print_op(msg=outter_item, msg2=inner_item)


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '2.json')
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
