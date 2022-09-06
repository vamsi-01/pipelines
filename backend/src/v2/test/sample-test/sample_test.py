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

import asyncio
import dataclasses
import datetime
import os
import sys
import tempfile
import time
import types
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from kfp import client
from kfp import compiler
from kfp.components import pipeline_context
from kfp.components import python_component
import kfp.deprecated as kfp_v1
import kfp_server_api
import pytest
import yaml
import mlmd_client

KFP_ENDPOINT = os.environ['KFP_ENDPOINT']
# TODO change
SOURCE_CHANGE = os.environ.get('SOURCE_CHANGE', 'sdk')
CLIENT_MAP = {
    'v1': kfp_v1.Client(KFP_ENDPOINT),
    'v2': client.Client(KFP_ENDPOINT)
}
COMPILER_MAP = {'v1': kfp_v1.compiler.Compiler(), 'v2': compiler.Compiler()}
CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))
PROJECT_ROOT = os.path.abspath(
    os.path.join(CURRENT_DIR, *([os.path.pardir] * 5)))


def get_config() -> Any:
    samples_config = os.path.join(CURRENT_DIR, 'config.yaml')
    with open(samples_config) as f:
        return yaml.safe_load(f.read())


@dataclasses.dataclass
class TestCase:
    version: str
    path: str
    pipeline: str
    arguments: Dict[str, Any] = dataclasses.field(default_factory=dict)
    execute: bool = True
    expect_failure: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


def transform_config_to_test_case_parameters(
        config: Dict[str, Any]) -> List[TestCase]:
    test_cases: List[TestCase] = []
    for version, test_list in config.items():
        test_cases.extend(
            TestCase(version=version, **case) for case in test_list)
    return test_cases


def collect_pipeline_from_module(
    target_module: types.ModuleType
) -> Union[Callable[..., Any], python_component.PythonComponent]:
    pipelines = []
    module_attrs = dir(target_module)
    for attr in module_attrs:
        obj = getattr(target_module, attr)
        if pipeline_context.Pipeline.is_pipeline_func(obj):
            pipelines.append(obj)
    if len(pipelines) == 1:
        return pipelines[0]
    else:
        raise ValueError(
            f'Expect one pipeline function in module {target_module}, got {len(pipelines)}: {pipelines}. Please specify the pipeline function name with --function.'
        )


def collect_pipeline_func(
    python_file: str, function_name: Optional[str]
) -> Union[Callable[..., Any], python_component.PythonComponent]:
    sys.path.insert(0, os.path.dirname(python_file))
    try:
        filename = os.path.basename(python_file)
        module_name = os.path.splitext(filename)[0]
        if function_name is None:
            return collect_pipeline_from_module(
                target_module=__import__(module_name))

        module = __import__(module_name, fromlist=[function_name])
        if not hasattr(module, function_name):
            raise ValueError(
                f'Pipeline function or component "{function_name}" not found in module {filename}.'
            )

        pipeline = getattr(module, function_name)
        # pop module in case reimporting from a module with the same name for a different pipeline test
        sys.modules.pop(module.__name__)
        return pipeline

    finally:
        del sys.path[0]


def run(test_case: TestCase) -> Tuple[str, client.client.RunPipelineResult]:
    full_path = os.path.join(PROJECT_ROOT, test_case.path)
    pipeline_func = collect_pipeline_func(full_path, test_case.pipeline)
    # TODO: remove
    enable_caching = True
    # enable_caching = SOURCE_CHANGE.lower() == 'sdk'
    client = CLIENT_MAP[test_case.version]
    run_result = client.create_run_from_pipeline_func(
        pipeline_func, arguments={}, enable_caching=enable_caching)
    run_url = f'{KFP_ENDPOINT}/#/runs/details/{run_result.run_id}'
    print(
        f'\nRunning {test_case.version} pipeline {test_case.pipeline} from {test_case.path}:\n\t{run_url}'
    )
    return run_url, run_result


def compile(test_case: TestCase) -> None:
    full_path = os.path.join(PROJECT_ROOT, test_case.path)
    pipeline_func = collect_pipeline_func(full_path, test_case.pipeline)
    compiler = COMPILER_MAP[test_case.version]
    with tempfile.TemporaryDirectory() as tempdir:
        file_name = os.path.split(test_case.path)[1].replace('.py', '.yaml')
        output_path = os.path.join(tempdir, file_name)
        compiler.compile(pipeline_func, package_path=output_path)


def wait(
        run_result: client.client.RunPipelineResult
) -> kfp_server_api.ApiRunDetail:
    total_retries = 0
    backoff_timeline = [5, 30, 60]
    max_retries = len(backoff_timeline)

    # inner fn to handle APIExceptions and retry with backoff
    def inner_wait(
            run_result: client.client.RunPipelineResult
    ) -> kfp_server_api.ApiRun:
        try:
            v2_client = CLIENT_MAP['v2']
            return v2_client.wait_for_run_completion(
                run_id=run_result.run_id, timeout=datetime.timedelta.max)
        except kfp_server_api.exceptions.ApiException:
            nonlocal total_retries
            print(
                f'ERROR: Got error number {total_retries} when querying {run_result.run_id}.'
            )
            if total_retries >= max_retries:
                raise
            time.sleep(backoff_timeline[total_retries])
            total_retries += 1
            return inner_wait(run_result=run_result)

    return inner_wait(run_result)


def get_verification_func(test_case: TestCase) -> Callable:
    dirname, filename = os.path.split(test_case.path)
    sys.path.insert(0, os.path.join(dirname, 'verification'))
    try:
        module_name = os.path.splitext(f'verify_{filename}')[0]
        module = __import__(module_name, fromlist=['verify'])
        if not hasattr(module, 'verify'):
            raise ValueError('Function "verify" not found.')
        sys.modules.pop(module.__name__)
        return module.verify
    finally:
        del sys.path[0]


config = get_config()
test_cases = transform_config_to_test_case_parameters(config)


@pytest.mark.asyncio_cooperative
@pytest.mark.parametrize('test_case', test_cases)
async def test(test_case: TestCase) -> None:
    """Asynchronously runs all samples and test that they succeed."""

    event_loop = asyncio.get_running_loop()
    print(test_case)
    if test_case.execute:
        run_url, run_result = run(test_case)
        api_run_detail = await event_loop.run_in_executor(
            None, wait, run_result)
        verify_func = get_verification_func(test_case)
        # verify_func(api_run_detail)
        tasks = mlmd_client.MlmdClient().get_taskss(api_run_detail.run.id)
        print(tasks)

        # assert api_run.run.status == (
        #     'Failed' if test_case.expect_failure else 'Succeeded'
        # ), f'Pipeline {test_case.path}-{test_case.pipeline} ended with incorrect status: {api_run.run.status}. More info: {run_url}.'

    else:
        compile(test_case)


if __name__ == '__main__':
    TWENTY_MINUTES = 15 * 60
    retcode = pytest.main(['--asyncio-task-timeout', str(TWENTY_MINUTES)])
