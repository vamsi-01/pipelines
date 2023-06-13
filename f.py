PERSISTENT_RESOURCE_ID = 'test-persistent-resource-no-accelerator'
#
import requests
import subprocess
import shlex


def get_access_token():
    command = shlex.split('gcloud auth print-access-token')
    output = subprocess.check_output(command).strip()
    return output.decode()


PROJECT_NUMBER = 186556260430
PERSISTENT_RESOURCE_REQUEST = {
    "display_name":
        "Test-Persistent-Resource",
    "resource_pools": [{
        "machine_spec": {
            "machine_type": "n1-highmem-4"
        },
        "replica_count": 4,
        "disk_spec": {
            "boot_disk_type": "pd-ssd",
            "boot_disk_size_gb": 100
        }
    }, {
        "machine_spec": {
            "machine_type": "n1-standard-4",
        },
        "replica_count": 4,
        "disk_spec": {
            "boot_disk_type": "pd-ssd",
            "boot_disk_size_gb": 100
        }
    }]
}
request_uri = f"https://us-central1-aiplatform.googleapis.com/v1beta1/projects/{PROJECT_NUMBER}/locations/us-central1/persistentResources"

response = requests.post(
    request_uri,
    params={"persistent_resource_id": PERSISTENT_RESOURCE_ID},
    json=PERSISTENT_RESOURCE_REQUEST,
    headers={
        'Authorization': 'Bearer ' + get_access_token(),
        'Content-Type': 'application/json'
    })

# print(response.text)
from google_cloud_pipeline_components.v1 import custom_job
from kfp import dsl
from kfp.dsl import Artifact
from kfp.dsl import Input
from kfp.dsl import Output
import kfp

kfp.__version__ = '2.0.0-rc.1'


@dsl.component
def sum_numbers(a: int = 1, b: int = 2) -> int:
    return a + b


comp = custom_job.create_custom_training_job_from_component(
    sum_numbers, persistent_resource_id=PERSISTENT_RESOURCE_ID)

from kfp import dsl


@dsl.pipeline
def my_pipeline():
    comp(project='271009669852', location='us-central1')


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
    pipeline_name = __file__.split('/')[-1].replace('_', '-').replace('.py', '')
    display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
    job_id = f'{pipeline_name}-{display_name}'
    aiplatform.PipelineJob(
        project='186556260430',
        template_path=ir_file,
        pipeline_root='gs://cjmccarthy-kfp-default-bucket',
        display_name=pipeline_name,
        job_id=job_id).submit()
    url = f'https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/{pipeline_name}-{display_name}?project=186556260430'
    webbrowser.open_new_tab(url)