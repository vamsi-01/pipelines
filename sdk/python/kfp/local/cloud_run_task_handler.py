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
import random
import string
from typing import List

from google.api_core import operation
from kfp.local import config
from kfp.local import status
from kfp.local import task_handler_interface


class CloudRunTaskHandler(task_handler_interface.ITaskHandler):
    """The task handler corresponding to DockerRunner."""

    def __init__(
        self,
        image: str,
        full_command: List[str],
        pipeline_root: str,
        runner: config.CloudRunRunner,
    ) -> None:
        self.image = image
        self.full_command = full_command
        self.pipeline_root = pipeline_root
        self.runner = runner

        from google.cloud import run_v2
        self.client = run_v2.JobsClient()
        self.parent = self.client.common_location_path(
            runner.project,
            runner.location,
        )

    def run(self) -> status.Status:
        """Runs the Cloud Run job and returns the status."""
        job_name = make_job(
            self.client,
            self.parent,
            self.image,
            self.full_command,
        )
        run_op = run_job(
            self.client,
            job_name,
        )
        print(f"Job name: {job_name}")
        print("TODO: stream logs")
        print(run_op.metadata.annotations)
        execution_id = run_op.metadata.annotations[
            'run.googleapis.com/execution-id']
        print('View logs on the console:',
              make_execution_url(
                  self.runner.location,
                  execution_id,
              ))
        is_success = wait_and_return_success(run_op)
        return status.Status.SUCCESS if is_success else status.Status.FAILURE


from google.cloud import logging_v2


def stream_cloud_run_logs(
    project: str,
    location: str,
    job_id: str,
):
    client = logging_v2.LoggingServiceV2Client()
    resource_names = [f"projects/{project}"]
    filter = f'resource.type="cloud_run_job" AND resource.labels.job_name="{job_id}" AND resource.labels.location="{location}"'

    for entry in client.list_log_entries(
            resource_names=resource_names, filter=filter):
        print(entry)


def make_job(
    client: 'JobsClient',
    parent: str,
    image: str,
    full_command: List[str],
) -> str:
    from google.cloud import run_v2
    from google.cloud.run_v2 import types

    job = run_v2.Job(
        template=types.ExecutionTemplate(
            template=types.TaskTemplate(containers=[
                types.Container(
                    image=image,
                    command=full_command,
                )
            ])))
    job_id = f'job-{gen_random_id()}'
    create_request = run_v2.CreateJobRequest(
        parent=parent,
        job=job,
        job_id=job_id,
    )

    create_op = client.create_job(request=create_request)
    return create_op.result().name


def run_job(client: 'JobsClient', job_name: str) -> None:
    from google.cloud import run_v2
    run_request = run_v2.RunJobRequest(name=job_name)
    return client.run_job(request=run_request)


def wait_and_return_success(run_op: operation.Operation) -> bool:
    try:
        for condition in run_op.result().conditions:
            if condition.type_ == 'Completed':
                return condition.state == 'CONDITION_SUCCEEDED'
    except:
        return False
    return False


def gen_random_id() -> str:
    return ''.join(random.choices(
        string.ascii_letters + string.digits,
        k=4,
    )).lower()


# make meaningful
def make_job_id() -> str:
    return f'job-{gen_random_id()}'


def make_execution_url(
    location: str,
    job_id: str,
) -> str:
    return f'https://console.cloud.google.com/run/jobs/executions/details/{location}/{job_id}/tasks'
