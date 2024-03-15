import shlex
import subprocess

import requests

location = 'us-central1'


def get_access_token():
    command = shlex.split('gcloud auth print-access-token')
    output = subprocess.check_output(command).strip()
    return output.decode()


PROJECT_NUMBER = 186556260430

project = 'managed-pipeline-test'
location = 'us-central1'

source = """
def main():
    import logging
    logging.error("foo")

main()
"""

# Configure the custom job
CUSTOM_JOB = {
    'displayName': 'my-test',
    'jobSpec': {
        "workerPoolSpecs": [{
            "machineSpec": {
                "machineType": "n1-standard-4"
            },
            "replicaCount": "1",
            "diskSpec": {
                "bootDiskType": "pd-standard",
                "bootDiskSizeGb": 100
            },
            "containerSpec": {
                "imageUri": "alpine",
                "command": [],
                "args": ["\"sleep 20\""]
            }
        }],
    }
}

parent = f"projects/{project}/locations/{location}"

request_uri = f'https://{location}-aiplatform.googleapis.com/v1beta1/projects/{PROJECT_NUMBER}/locations/{location}/customJobs'

response = requests.post(
    request_uri,
    params={
        'parent': parent,
    },
    json=CUSTOM_JOB,
    headers={
        'Authorization': 'Bearer ' + get_access_token(),
        'Content-Type': 'application/json'
    })

print(response.text)
import webbrowser
import json

name = json.loads(response.text)['name'].split('/')[-1]
webbrowser.open(
    f'https://pantheon.corp.google.com/vertex-ai/locations/us-central1/training/{name}/cpu?e=13802955&mods=-ai_platform_fake_service&project=managed-pipeline-test'
)
