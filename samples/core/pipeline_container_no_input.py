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
import os

from kfp import compiler
from kfp import dsl


@dsl.container_component
def container_no_input():
    return dsl.ContainerSpec(
        image='python:3.7',
        command=['echo', 'hello world'],
        args=[],
    )


@dsl.pipeline(name='v2-container-component-no-input')
def pipeline_container_no_input():
    container_no_input()


def verify(run) -> None:
    assert run.status == Succeeded
    # t.assertEqual(
    #     {
    #         'container-no-input':
    #             KfpTask(
    #                 name='container-no-input',
    #                 type='system.ContainerExecution',
    #                 state=Execution.State.COMPLETE,
    #                 inputs=TaskInputs(parameters={}, artifacts=[]),
    #                 outputs=TaskOutputs(parameters={}, artifacts=[]))
    #     },
    #     tasks,
    # )