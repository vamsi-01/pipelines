# Copyright 2019 The Kubeflow Authors
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

import kfp
import kfp.dsl as dsl
from kfp import components


@components.create_component_from_func
def write_to_volume():
    with open("/mnt/file.txt", "w") as file:
        file.write("Hello world")


@dsl.pipeline(name="volumeop-basic",
              description="A Basic Example on VolumeOp Usage.")
def volumeop_basic(size: str = "1Gi"):
    vop = dsl.VolumeOp(name="create-pvc",
                       resource_name="my-pvc",
                       modes=dsl.VOLUME_MODE_RWO,
                       size=size)

    write_to_volume().add_pvolumes({"/mnt'": vop.volume})


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from kfp import compiler

    from kfp import Client

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=volumeop_basic,
                                package_path=ir_file)
    pipeline_name = __file__.split('/')[-1].replace('_',
                                                    '-').replace('.py', '')
    display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
    job_id = f'{pipeline_name}-{display_name}'
    endpoint = 'https://75167a6cffcb723c-dot-us-central1.pipelines.googleusercontent.com'
    kfp_client = Client(host=endpoint)
    run = kfp_client.create_run_from_pipeline_package(ir_file, arguments={})
    url = f'{endpoint}/#/runs/details/{run.run_id}'
    print(url)
    webbrowser.open_new_tab(url)