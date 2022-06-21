import os
import re
import sys
import tempfile
import types
from typing import Any, Callable, Dict, Optional, Union

from absl.testing import parameterized
from kfp import compiler
from kfp import components
from kfp.components import pipeline_context
from kfp.components import python_component
import yaml

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), 'test_data')
PIPELINES_TEST_DATA_DIR = os.path.join(TEST_DATA_DIR, 'pipelines')
SUPPORTED_COMPONENTS_TEST_DATA_DIR = os.path.join(TEST_DATA_DIR, 'components')
V1_COMPONENTS_TEST_DATA_DIR = os.path.join(TEST_DATA_DIR, 'v1_component_yaml')
PIPELINE_TEST_CASES = [
    'pipeline_with_importer',
    'pipeline_with_ontology',
    'pipeline_with_if_placeholder',
    'pipeline_with_concat_placeholder',
    'pipeline_with_resource_spec',
    'pipeline_with_various_io_types',
    'pipeline_with_reused_component',
    'pipeline_with_after',
    'pipeline_with_condition',
    'pipeline_with_nested_conditions',
    'pipeline_with_nested_conditions_yaml',
    'pipeline_with_loops',
    'pipeline_with_nested_loops',
    'pipeline_with_loops_and_conditions',
    'pipeline_with_params_containing_format',
    'lightweight_python_functions_pipeline',
    'lightweight_python_functions_with_outputs',
    'xgboost_sample_pipeline',
    'pipeline_with_metrics_outputs',
    'pipeline_with_exit_handler',
    'pipeline_with_env',
    'component_with_optional_inputs',
    'pipeline_with_gcpc_types',
    'pipeline_with_placeholders',
    'pipeline_with_task_final_status',
    'pipeline_with_task_final_status_yaml',
    'component_with_pip_index_urls',
]
SUPPORTED_COMPONENT_TEST_CASES = [
    'add_numbers',
    'component_with_pip_install',
    'concat_message',
    'dict_input',
    'identity',
    'nested_return',
    'output_artifact',
    'output_metrics',
    'preprocess',
]

V1_COMPONENT_YAML_TEST_CASES = [
    'concat_placeholder_component.yaml',
    'ingestion_component.yaml',
    'serving_component.yaml',
    'if_placeholder_component.yaml',
    'trainer_component.yaml',
    'add_component.yaml',
]


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
    python_file: str,
    function_name: Optional[str] = None
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

        return getattr(module, function_name)

    finally:
        del sys.path[0]


def ignore_kfp_version_helper(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Ignores kfp sdk versioning in command.

    Takes in a YAML input and ignores the kfp sdk versioning in command
    for comparison between compiled file and goldens.
    """
    pipeline_spec = spec.get('pipelineSpec', spec)

    if 'executors' in pipeline_spec['deploymentSpec']:
        for executor in pipeline_spec['deploymentSpec']['executors']:
            pipeline_spec['deploymentSpec']['executors'][
                executor] = yaml.safe_load(
                    re.sub(
                        r"'kfp==(\d+).(\d+).(\d+)(-[a-z]+.\d+)?'", 'kfp',
                        yaml.dump(
                            pipeline_spec['deploymentSpec']['executors']
                            [executor],
                            sort_keys=True)))
    return spec


def load_compiled_file(filename: str) -> Dict[str, Any]:
    with open(filename) as f:
        contents = yaml.safe_load(f)
        pipeline_spec = contents[
            'pipelineSpec'] if 'pipelineSpec' in contents else contents
        # ignore the sdkVersion
        del pipeline_spec['sdkVersion']
        return ignore_kfp_version_helper(contents)


class ReadWriteEqualityTest(parameterized.TestCase):

    def _compile_and_load_component(
        self, compilable: Union[Callable[..., Any],
                                python_component.PythonComponent]):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file = os.path.join(tmp_dir, 're_compiled_output.yaml')
            compiler.Compiler().compile(compilable, tmp_file)
            return components.load_component_from_file(tmp_file)

    def _compile_and_read_yaml(
        self, compilable: Union[Callable[..., Any],
                                python_component.PythonComponent]):
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file = os.path.join(tmp_dir, 're_compiled_output.yaml')
            compiler.Compiler().compile(compilable, tmp_file)
            return load_compiled_file(tmp_file)

    def _read_write_read_is_lossless(self, yaml_file: str):
        """Loads a component from a YAML file, re-compiles it, and then asserts
        the YAML is equal. Tests that the YAML serialization and
        deserialization logic is consistent.

        Args:
            yaml_file (str): IR YAML file path.
        """
        original_component = components.load_component_from_file(yaml_file)
        reloaded_component = self._compile_and_load_component(
            original_component)
        self.assertEqual(original_component.component_spec,
                         reloaded_component.component_spec)

    def _test_compile_equals_snapshot(self,
                                      python_file: str,
                                      yaml_file: str,
                                      function_name: Optional[str] = None):
        """Compiles a pipeline from a module and asserts the compiled result is
        equal to the golden snapshot.

        Args:
            python_file (str): The Python file.
            yaml_file (str): The golden snapshot YAML file.
            function_name (str, optional): The function name to collect. Defaults to None.
        """
        pipeline = collect_pipeline_func(
            python_file, function_name=function_name)
        compiled_result = self._compile_and_read_yaml(pipeline)
        golden_result = load_compiled_file(yaml_file)
        self.assertEqual(compiled_result, golden_result)

    @parameterized.parameters(V1_COMPONENT_YAML_TEST_CASES)
    def test_ir_component(self, file: str):
        yaml_file = os.path.join(V1_COMPONENTS_TEST_DATA_DIR, file)
        self._read_write_read_is_lossless(yaml_file=yaml_file)

    @parameterized.parameters(SUPPORTED_COMPONENT_TEST_CASES)
    def test_v1_components(self, file: str):
        yaml_file = os.path.join(SUPPORTED_COMPONENTS_TEST_DATA_DIR,
                                 f'{file}.yaml')
        python_file = os.path.join(SUPPORTED_COMPONENTS_TEST_DATA_DIR,
                                   f'{file}.py')
        self._read_write_read_is_lossless(yaml_file=yaml_file)
        self._test_compile_equals_snapshot(
            python_file=python_file, yaml_file=yaml_file, function_name=file)

    @parameterized.parameters(PIPELINE_TEST_CASES)
    def test_pipelines(self, file: str):
        yaml_file = os.path.join(PIPELINES_TEST_DATA_DIR, f'{file}.yaml')
        python_file = os.path.join(PIPELINES_TEST_DATA_DIR, f'{file}.py')
        self._test_compile_equals_snapshot(
            python_file=python_file, yaml_file=yaml_file)
