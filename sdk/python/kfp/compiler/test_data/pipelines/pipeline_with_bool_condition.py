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

from kfp.deprecated.v2 import dsl


@dsl.component
def true_op() -> bool:
    return True


@dsl.component
def print_op(msg: str):
    """Print a message."""
    print(msg)


@dsl.pipeline(name='pipeline-with-bool-condition', pipeline_root='dummy_root')
def my_pipeline(text: str = 'heads'):

    with dsl.Condition(text == 'heads'):
        print_op(msg=text)

    true_task = true_op()
    with dsl.Condition(true_task.output == True):
        print_op(msg=text)

    with dsl.Condition('heads' == 'heads'):
        print_op(msg=text)


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=my_pipeline,
        pipeline_parameters={'text': 'heads'},
        package_path=__file__.replace('.py', '.yaml'))
