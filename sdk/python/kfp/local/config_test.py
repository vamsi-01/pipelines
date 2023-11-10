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
"""Tests for config.py."""
import unittest

from kfp import local
from kfp.local import config


class LocalRunnerConfigSingleton(unittest.TestCase):

    def setUp(self):
        config.LocalExecutionConfig.instance = None

    def test_one_instance(self):
        """Test instance attributes with one init()."""
        config.LocalExecutionConfig(
            pipeline_root='my/local/root',
            runner=local.SubprocessRunner(use_venv=True),
            cleanup=True,
        )

        instance = config.LocalExecutionConfig.instance

        self.assertEqual(instance.pipeline_root, 'my/local/root')
        self.assertEqual(instance.runner, local.SubprocessRunner(use_venv=True))
        self.assertIs(instance.cleanup, True)

    def test_one_instance(self):
        """Test instance attributes with one init()."""
        config.LocalExecutionConfig(
            pipeline_root='my/local/root',
            runner=local.SubprocessRunner(),
            cleanup=True,
        )
        config.LocalExecutionConfig(
            pipeline_root='other/local/root',
            runner=local.SubprocessRunner(use_venv=False),
            cleanup=False,
        )

        instance = config.LocalExecutionConfig.instance

        self.assertEqual(instance.pipeline_root, 'other/local/root')
        self.assertEqual(instance.runner,
                         local.SubprocessRunner(use_venv=False))
        self.assertIs(instance.cleanup, False)


class TestInitCalls(unittest.TestCase):

    def setUp(self):
        config.LocalExecutionConfig.instance = None

    def test_one_instance(self):
        """Test instance attributes with one init()."""
        local.init(
            pipeline_root='my/local/root',
            runner=local.SubprocessRunner(use_venv=True),
            cleanup=True,
        )

        instance = config.LocalExecutionConfig.instance

        self.assertEqual(instance.pipeline_root, 'my/local/root')
        self.assertEqual(instance.runner, local.SubprocessRunner(use_venv=True))
        self.assertIs(instance.cleanup, True)

    def test_one_instance(self):
        """Test instance attributes with one init()."""
        local.init(
            pipeline_root='my/local/root',
            runner=local.SubprocessRunner(),
            cleanup=True,
        )
        local.init(
            pipeline_root='other/local/root',
            runner=local.SubprocessRunner(use_venv=False),
            cleanup=False,
        )

        instance = config.LocalExecutionConfig.instance

        self.assertEqual(instance.pipeline_root, 'other/local/root')
        self.assertEqual(instance.runner,
                         local.SubprocessRunner(use_venv=False))
        self.assertIs(instance.cleanup, False)


if __name__ == '__main__':
    unittest.main()
