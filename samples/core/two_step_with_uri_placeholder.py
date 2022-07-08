# Copyright 2021 The Kubeflow Authors
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
"""Two step v2-compatible pipeline with URI placeholders."""
from kfp import components
from kfp import dsl

write_to_gcs_op = components.load_component_from_text("""
name: write-to-gcs
inputs:
- {name: msg, type: String, description: 'Content to be written to GCS'}
outputs:
- {name: artifact, type: Artifact, description: 'GCS file path'}
implementation:
  container:
    image: google/cloud-sdk:slim
    command:
    - sh
    - -c
    - |
      set -e -x
      echo "$0" | gsutil cp - "$1"
    - {inputValue: msg}
    - {outputUri: artifact}
""")

read_from_gcs_op = components.load_component_from_text("""
name: read-from-gcs
inputs:
- {name: artifact, type: Artifact, description: 'GCS file path'}
implementation:
  container:
    image: google/cloud-sdk:slim
    command:
    - sh
    - -c
    - |
      set -e -x
      gsutil cat "$0"
    - {inputUri: artifact}
""")


@dsl.pipeline(name='two-step-with-uri-placeholders')
def two_step_with_uri_placeholder(msg: str = 'Hello world!'):
    write_to_gcs = write_to_gcs_op(msg=msg)
    read_from_gcs = read_from_gcs_op(artifact=write_to_gcs.outputs['artifact'])


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from kfp import client
    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(
        pipeline_func=two_step_with_uri_placeholder, package_path=ir_file)
    pipeline_name = __file__.split('/')[-1].replace('_', '-').replace('.py', '')
    display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
    job_id = f'{pipeline_name}-{display_name}'
    endpoint = 'https://4e18c21c9d33d20f-dot-datalab-vm-staging.googleusercontent.com'
    kfp_client = client.Client(host=endpoint)
    run = kfp_client.create_run_from_pipeline_package(ir_file)
    url = f'{endpoint}/#/runs/details/{run.run_id}'
    print(url)
    webbrowser.open_new_tab(url)
