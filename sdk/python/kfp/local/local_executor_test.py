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
"""Tests for local_executor_test.py."""
import contextlib
import os
import unittest

from kfp import dsl
from kfp import local
from kfp.local import configuration
from kfp.local import local_executor


class TestRunSingleComponent(unittest.TestCase):

    def tearDown(self):
        configuration.LocalRunnerConfig.instance = None

    def test_initialized(self):
        local.init()

        @dsl.component
        def identity(string: str) -> str:
            return string

        # capture + discard stdout
        with open(os.devnull, 'w') as dev_null:
            with contextlib.redirect_stdout(dev_null):

                local_executor.run_single_component(
                    identity.pipeline_spec,
                    arguments={'string': 'foo'},
                )

    def test_not_initialized(self):

        @dsl.component
        def identity(string: str) -> str:
            return string

        with self.assertRaisesRegex(
                RuntimeError,
                r'You must initiatize the local execution session using kfp\.local\.init\(\)\.'
        ):
            local_executor.run_single_component(
                identity.pipeline_spec,
                arguments={'string': 'foo'},
            )


if __name__ == '__main__':
    unittest.main()
