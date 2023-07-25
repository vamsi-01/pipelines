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

from kfp import compiler
from kfp import dsl
from kfp.dsl import component


@component
def flip_coin_op() -> str:
    """Flip a coin and output heads or tails randomly."""
    import random
    result = 'heads' if random.randint(0, 1) == 0 else 'tails'
    return result


@component
def print_op(msg: str):
    """Print a message."""
    print(msg)


@dsl.pipeline(name='nested-conditions-pipeline')
def my_pipeline():
    flip1 = flip_coin_op()
    print_op(msg=flip1.output)
    flip2 = flip_coin_op()
    print_op(msg=flip2.output)

    with dsl.Condition(flip1.output != 'no-such-result'):  # always true
        flip3 = flip_coin_op()
        print_op(msg=flip3.output)

        with dsl.Condition(flip2.output == flip3.output):
            flip4 = flip_coin_op()
            print_op(msg=flip4.output)


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        package_path=__file__.replace('.py', '.yaml'))
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