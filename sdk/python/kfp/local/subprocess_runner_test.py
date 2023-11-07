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
"""Tests for the implementation of the subprocess runner."""
import contextlib
import io
import unittest
from unittest import mock

from kfp.local import subprocess_runner


class TestUseCurrentPythonExecutable(unittest.TestCase):

    @mock.patch('sys.executable', '/foo/bar/python3')
    def test(self):
        full_command = ['python3 -c "from kfp import dsl"']
        actual = subprocess_runner.use_current_python_executable(full_command)
        expected = ['/foo/bar/python3 -c "from kfp import dsl"']
        self.assertEqual(actual, expected)


class TestRunLocalSubproces(unittest.TestCase):

    def test_log_to_stdout(self):
        buffer = io.StringIO()

        with contextlib.redirect_stdout(buffer):
            subprocess_runner.run_local_subprocess(['echo', 'foo'])

        output = buffer.getvalue().strip()

        self.assertEqual(output, 'foo')

    def test_no_log_to_stdout(self):
        buffer = io.StringIO()

        with contextlib.redirect_stdout(buffer):
            subprocess_runner.run_local_subprocess(
                ['echo', 'foo'],
                log_to_stdout=False,
            )

        output = buffer.getvalue().strip()

        self.assertEqual(output, '')

    def test_env_var_set(self):
        buffer = io.StringIO()

        with contextlib.redirect_stdout(buffer):
            subprocess_runner.run_local_subprocess(
                [
                    'python3',
                    '-c',
                    'import os; print(os.environ["LOCAL_SESSION"])',
                ],
                log_to_stdout=True,
            )

        output = buffer.getvalue().strip()

        self.assertEqual(output, 'true')

    def test_raises_log_to_stdout(self):
        buffer = io.StringIO()

        with contextlib.redirect_stdout(buffer):
            with self.assertRaisesRegex(RuntimeError, r'foobar'):
                subprocess_runner.run_local_subprocess(
                    [
                        'python3',
                        '-c',
                        'raise Exception("foobar")',
                    ],
                    log_to_stdout=True,
                )

        output = buffer.getvalue().strip()

        self.assertEqual(output, '')

    def test_raises_log_to_stdout(self):
        buffer = io.StringIO()

        with contextlib.redirect_stdout(buffer):
            with self.assertRaisesRegex(RuntimeError, r'foobar'):
                subprocess_runner.run_local_subprocess(
                    [
                        'python3',
                        '-c',
                        'raise Exception("foobar")',
                    ],
                    log_to_stdout=False,
                )

        output = buffer.getvalue().strip()

        self.assertEqual(output, '')
