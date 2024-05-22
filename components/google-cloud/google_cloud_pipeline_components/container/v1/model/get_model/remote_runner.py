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
"""Remote runner for Get Model based on the Vertex AI SDK."""

from google.api_core.client_options import ClientOptions
from google.cloud import aiplatform_v1 as aip_v1
from google_cloud_pipeline_components.container.utils import artifact_utils
from google_cloud_pipeline_components.container.utils import error_surfacing
from google_cloud_pipeline_components.proto import task_error_pb2
from google_cloud_pipeline_components.types import artifact_types


def get_model(
    executor_input,
    model_name: str,
    project: str,
    location: str,
) -> None:
  """Get model."""
  task_error = task_error_pb2.TaskError()
  if not location or not project:
    model_name_error_message = (
        'Model resource name must be in the format'
        ' projects/{project}/locations/{location}/models/{model_name}'
    )
    task_error.error_message = model_name_error_message
    error_surfacing.write_user_defined_error(executor_input, task_error)
    raise ValueError(model_name_error_message)
  api_endpoint = location + '-aiplatform.googleapis.com'
  vertex_uri_prefix = f'https://{api_endpoint}/v1/'
  model_resource_name = (
      f'projects/{project}/locations/{location}/models/{model_name}'
  )

  client_options = ClientOptions(api_endpoint=api_endpoint)
  client = aip_v1.ModelServiceClient(client_options=client_options)
  request = aip_v1.GetModelRequest(name=model_resource_name)
  try:
    get_model_response = client.get_model(request)
  except Exception as e:
    task_error.error_message = str(e)
    error_surfacing.write_user_defined_error(executor_input, task_error)
    raise
  resp_model_name_without_version = get_model_response.name.split('@', 1)[0]
  model_resource_name = (
      f'{resp_model_name_without_version}@{get_model_response.version_id}'
  )

  vertex_model = artifact_types.VertexModel.create(
      'model', vertex_uri_prefix + model_resource_name, model_resource_name
  )
  artifact_utils.update_output_artifacts(executor_input, [vertex_model])
