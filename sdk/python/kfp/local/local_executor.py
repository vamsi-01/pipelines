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
"""Code for executing DSL code locally."""
from typing import Any, Dict

# TODO: nest
from kfp import local
from kfp.local import configuration
from kfp.local import utils
from kfp.local.container_runner import ContainerRunnerImpl
from kfp.local.local_task import LocalTask
from kfp.local.subprocess_runner import SubprocessRunnerImpl
from kfp.pipeline_spec import pipeline_spec_pb2

# [x] TODO: finish executor input
# [x] TODO: finish executor output
# [x] TODO: output surfacing
# [x] TODO: non-Artifact artifact
# [x] TODO: tests
# [x] TODO: defaults in executor input
# [] TODO: container component tests
# [] TODO: validate arguments
# [] TODO: inject task root into executor input
# [] TODO: finish placeholder resolution


def validate_arguments():
    # TODO: fill in
    ...


def run_single_component(
    pipeline_spec: pipeline_spec_pb2.PipelineSpec,
    arguments: Dict[str, Any],
) -> LocalTask:
    if configuration.LocalRunnerConfig.instance is None:
        raise RuntimeError(
            f'You must initiatize the local execution session using {local.__name__}.{local.init.__name__}().'
        )

    return run_single_component_implementation(
        pipeline_spec=pipeline_spec,
        arguments=arguments,
        pipeline_root=configuration.LocalRunnerConfig.instance.pipeline_root,
    )


def run_single_component_implementation(
    pipeline_spec: pipeline_spec_pb2.PipelineSpec,
    arguments: Dict[str, Any],
    pipeline_root: str,
) -> LocalTask:
    _, component_spec = list(pipeline_spec.components.items())[0]

    executor_input = utils.construct_executor_input(
        component_spec=component_spec,
        arguments=arguments,
        pipeline_root=pipeline_root,
    )

    executor_spec = pipeline_spec.deployment_spec['executors'][
        component_spec.executor_label]

    full_command = list(executor_spec['container']['command']) + list(
        executor_spec['container']['args'])
    full_command = utils.replace_placeholders(
        full_command=full_command,
        executor_input=executor_input,
    )

    if isinstance(configuration.LocalRunnerConfig.instance.runner,
                  local.SubprocessRunner):
        runner = SubprocessRunnerImpl(
            full_command=full_command,
            pipeline_root=pipeline_root,
            executor_input=executor_input,
            component_spec=component_spec,
        )
        return runner.run()
    elif isinstance(configuration.LocalRunnerConfig.instance.runner,
                    local.ContainerRunner):
        runner = ContainerRunnerImpl(
            full_command=full_command,
            image=executor_spec['container']['image'],
            pipeline_root=pipeline_root,
            executor_input=executor_input,
            component_spec=component_spec,
        )
        return runner.run()
    else:
        raise ValueError(f'Got unknown argument for runner: {local._runner}')
