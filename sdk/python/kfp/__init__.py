# Copyright 2018-2022 The Kubeflow Authors
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

# `kfp` is a namespace package.
# https://packaging.python.org/guides/packaging-namespace-packages/#pkgutil-style-namespace-packages
__path__ = __import__('pkgutil').extend_path(__path__, __name__)

__version__ = '2.7.0'

import functools
import sys
from typing import Callable, Dict, List
import warnings

if sys.version_info < (3, 8):
    warnings.warn(
        ('Python 3.7 has reached end-of-life. KFP will drop support for Python 3.7 on April 23, 2024. To use new versions of the KFP SDK after that date, you will need to upgrade to Python >= 3.8. See https://devguide.python.org/versions/ for more details.'
        ),
        FutureWarning,
        stacklevel=2,
    )

TYPE_CHECK = True

import os

# compile-time only dependencies
if os.environ.get('_KFP_RUNTIME', 'false') != 'true':
    # make `from kfp import components` and `from kfp import dsl` valid;
    # related to namespace packaging issue
    from kfp import components  # noqa: keep unused import
    from kfp import dsl  # noqa: keep unused import
    from kfp.client import Client  # noqa: keep unused import

import abc


class PlatformInterface(abc.ABC):

    @property
    @abc.abstractclassmethod
    def task_methods(self) -> str:
        pass

    @property
    @abc.abstractclassmethod
    def provider_name(self) -> str:
        pass


class DynamicPlatformMethods:

    def __init__(
        self,
        task: 'PipelineTask',
        task_methods: List[Callable],
    ) -> None:
        self.task = task
        self.task_methods = {m.__name__: m for m in task_methods}

    def __getattr__(self, method_name: str) -> Callable:
        if method_name in self.task_methods:
            return functools.partial(self.task_methods[method_name], task=self)
        else:
            return getattr(self.task, method_name)


_REGISTERED_PLATFORMS: Dict[str, PlatformInterface] = {}


def register_platform(platform_task_interface: PlatformInterface) -> None:
    _REGISTERED_PLATFORMS[
        platform_task_interface.provider_name] = platform_task_interface
