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
"""Objects for configuring local execution."""
import unittest
from unittest import mock

from docker import errors as docker_errors
from kfp import local
from kfp.local import config


class LocalRunnerConfigSingleton(unittest.TestCase):

    def setUp(self):
        config.LocalRunnerConfig.instance = None

    def test_one_instance(self):
        """Test instance attributes with one init()."""
        config.LocalRunnerConfig(
            pipeline_root='my/local/root',
            runner=local.SubprocessRunner(use_venv=True),
            cleanup=True,
        )

        instance = config.LocalRunnerConfig.instance

        self.assertEqual(instance.pipeline_root, 'my/local/root')
        self.assertEqual(instance.runner, local.SubprocessRunner(use_venv=True))
        self.assertIs(instance.cleanup, True)

    def test_one_instance(self):
        """Test instance attributes with one init()."""
        config.LocalRunnerConfig(
            pipeline_root='my/local/root',
            runner=local.ContainerRunner(),
            cleanup=True,
        )
        config.LocalRunnerConfig(
            pipeline_root='other/local/root',
            runner=local.SubprocessRunner(use_venv=False),
            cleanup=False,
        )

        instance = config.LocalRunnerConfig.instance

        self.assertEqual(instance.pipeline_root, 'other/local/root')
        self.assertEqual(instance.runner,
                         local.SubprocessRunner(use_venv=False))
        self.assertIs(instance.cleanup, False)


class TestInitCalls(unittest.TestCase):

    def setUp(self):
        config.LocalRunnerConfig.instance = None

    def test_one_instance(self):
        """Test instance attributes with one init()."""
        local.init(
            pipeline_root='my/local/root',
            runner=local.SubprocessRunner(use_venv=True),
            cleanup=True,
        )

        instance = config.LocalRunnerConfig.instance

        self.assertEqual(instance.pipeline_root, 'my/local/root')
        self.assertEqual(instance.runner, local.SubprocessRunner(use_venv=True))
        self.assertIs(instance.cleanup, True)

    def test_one_instance(self):
        """Test instance attributes with one init()."""
        local.init(
            pipeline_root='my/local/root',
            runner=local.ContainerRunner(),
            cleanup=True,
        )
        local.init(
            pipeline_root='other/local/root',
            runner=local.SubprocessRunner(use_venv=False),
            cleanup=False,
        )

        instance = config.LocalRunnerConfig.instance

        self.assertEqual(instance.pipeline_root, 'other/local/root')
        self.assertEqual(instance.runner,
                         local.SubprocessRunner(use_venv=False))
        self.assertIs(instance.cleanup, False)


class TestContainerRunner(unittest.TestCase):

    @mock.patch('docker.from_env')
    def test_validate_docker_is_running(self, mock_docker_from_env):
        mock_docker_from_env.side_effect = docker_errors.DockerException

        with self.assertRaisesRegex(docker_errors.DockerException,
                                    r'Docker is not running\..* try again\.'):
            local.ContainerRunner().validate_docker_is_running()

    def test_validate_docker_py_lib_is_installed_throws(self):
        with mock.patch('builtins.__import__') as mock_import:

            def side_effect(name, *args):
                if name == 'docker':
                    raise ModuleNotFoundError("No module named 'docker'")
                return original_import(name, *args)

            original_import = __import__
            mock_import.side_effect = side_effect

            with self.assertRaisesRegex(
                    ModuleNotFoundError,
                    'The Docker SDK for Python is not installed\. Please run `pip install docker` and try again\.'
            ):
                local.ContainerRunner().validate_docker_py_lib_is_installed()


if __name__ == '__main__':
    unittest.main()
