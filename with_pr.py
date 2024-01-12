PERSISTENT_RESOURCE_ID = 'vpp-persistent-resource-no-accelerator'

# import shlex
# import subprocess

# import requests

# location = 'us-central1'

# def get_access_token():
#     command = shlex.split('gcloud auth print-access-token')
#     output = subprocess.check_output(command).strip()
#     return output.decode()

# PROJECT_NUMBER = 1001017339809
# PERSISTENT_RESOURCE_REQUEST = {
#     'display_name':
#         'Test-Persistent-Resource',
#     'resource_pools': [{
#         'machine_spec': {
#             'machine_type': 'n1-highmem-4'
#         },
#         'replica_count': 4,
#         'disk_spec': {
#             'boot_disk_type': 'pd-ssd',
#             'boot_disk_size_gb': 100
#         }
#     }, {
#         'machine_spec': {
#             'machine_type': 'n1-standard-4',
#         },
#         'replica_count': 4,
#         'disk_spec': {
#             'boot_disk_type': 'pd-ssd',
#             'boot_disk_size_gb': 100
#         }
#     }]
# }
# request_uri = f'https://{location}-aiplatform.googleapis.com/v1beta1/projects/{PROJECT_NUMBER}/locations/{location}/persistentResources'

# response = requests.post(
#     request_uri,
#     params={
#         'persistent_resource_id': PERSISTENT_RESOURCE_ID,
#     },
#     json=PERSISTENT_RESOURCE_REQUEST,
#     headers={
#         'Authorization': 'Bearer ' + get_access_token(),
#         'Content-Type': 'application/json'
#     })

# print(response.text)
from google_cloud_pipeline_components.v1 import custom_job
from kfp import dsl
from kfp.dsl import Artifact
from kfp.dsl import Input
from kfp.dsl import Output
import kfp

from kfp import dsl
from kfp.dsl import *
from typing import *


@dsl.component
def identity(string: str) -> str:
    return string


identity_new = custom_job.create_custom_training_job_from_component(
    identity,
    persistent_resource_id=PERSISTENT_RESOURCE_ID,
)


@dsl.pipeline
def my_pipeline(string: str = 'string'):
    op1 = identity_new(
        string=string,
        project='186556260430',
        location='us-central1',
    ).set_caching_options(False)

    op1 = identity_new(
        string=op1.outputs['Output'],
        project='186556260430',
        location='us-central1',
    ).set_caching_options(False)


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
        location='us-central1',
        project='186556260430',
        template_path=ir_file,
        pipeline_root='gs://cjmccarthy-managed-pipelines-test',
        display_name=pipeline_name,
        job_id=job_id).submit()
    url = f'https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/{pipeline_name}-{display_name}?project=186556260430'
    webbrowser.open_new_tab(url)
