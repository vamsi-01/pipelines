# Copyright 2021 The Kubeflow Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Google Cloud Pipeline Components."""
import sys
from typing import Any
import warnings

from google_cloud_pipeline_components.version import __version__

if sys.version_info < (3, 8):
    warnings.warn(
        ('Python 3.7 has reached end-of-life. Google Cloud Pipeline Components'
         ' will drop support for Python 3.7 on April 23, 2024. To use new'
         ' versions of the KFP SDK after that date, you will need to upgrade'
         ' to Python >= 3.8. See https://devguide.python.org/versions/ for'
         ' more details.'),
        FutureWarning,
        stacklevel=2,
    )


def set_service_account(task, service_account: str):
    pass


def set_worker_pool_specs(task, worker_pool_specs: Any):
    pass


import kfp


class KubernetesPlatform(kfp.PlatformInterface):
    provider_name = 'vertex'
    task_methods = [set_service_account]
