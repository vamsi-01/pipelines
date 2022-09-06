# # Copyright 2021 The Kubeflow Authors
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# #      http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.

# from __future__ import annotations

# from dataclasses import asdict
# from dataclasses import dataclass
# from typing import Optional

# from google.protobuf.json_format import MessageToDict
# from ml_metadata import metadata_store
# from ml_metadata.metadata_store.metadata_store import ListOptions
# from ml_metadata.proto import Execution
# from ml_metadata.proto import metadata_store_pb2


# @dataclass
# class KfpArtifact:
#     name: str
#     uri: str
#     type: str
#     metadata: dict

#     @classmethod
#     def new(
#         cls,
#         mlmd_artifact: metadata_store_pb2.Artifact,
#         mlmd_artifact_type: metadata_store_pb2.ArtifactType,
#         mlmd_event: metadata_store_pb2.Event,
#     ):
#         # event path is conceptually input/output name in a task
#         # ref: https://github.com/google/ml-metadata/blob/78ea886c18979d79f3c224092245873474bfafa2/ml_metadata/proto/metadata_store.proto#L169-L180
#         artifact_name = mlmd_event.path.steps[0].key
#         # The original field is custom_properties, but MessageToDict converts it
#         # to customProperties.
#         metadata = simplify_proto_struct(
#             MessageToDict(mlmd_artifact).get('customProperties', {}))
#         return cls(
#             name=artifact_name,
#             type=mlmd_artifact_type.name,
#             uri=mlmd_artifact.uri,
#             metadata=metadata)


# @dataclass
# class TaskInputs:
#     parameters: dict
#     artifacts: list[KfpArtifact]


# @dataclass
# class TaskOutputs:
#     parameters: dict
#     artifacts: list[KfpArtifact]


# @dataclass
# class KfpTask:
#     """A KFP runtime task."""
#     name: str
#     type: str
#     state: int
#     inputs: TaskInputs
#     outputs: TaskOutputs
#     children: Optional[dict[str, KfpTask]] = None

#     def get_dict(self):
#         # Keep inputs and outputs keys, but ignore other zero values.
#         ignore_zero_values_except_io = lambda x: {
#             k: v for (k, v) in x if k in ['inputs', 'outputs'] or v
#         }
#         d = asdict(self, dict_factory=ignore_zero_values_except_io)
#         # remove uri, because they are not deterministic
#         for artifact in d.get('inputs', {}).get('artifacts', []):
#             artifact.pop('uri')
#         for artifact in d.get('outputs', {}).get('artifacts', []):
#             artifact.pop('uri')
#         # children should be accessed separately
#         if d.get('children') is not None:
#             d.pop('children')
#         return d

#     def __repr__(self, depth=1):
#         return_string = [str(self.get_dict())]
#         if self.children:
#             for child in self.children.values():
#                 return_string.extend(
#                     ['\n', '--' * depth,
#                      child.__repr__(depth + 1)])
#         return ''.join(return_string)

#     @classmethod
#     def new(
#         cls,
#         execution: metadata_store_pb2.Execution,
#         execution_types_by_id: dict[int, metadata_store_pb2.ExecutionType],
#         events_by_execution_id: dict[int, list[metadata_store_pb2.Event]],
#         artifacts_by_id: dict[int, metadata_store_pb2.Artifact],
#         artifact_types_by_id: dict[int, metadata_store_pb2.ArtifactType],
#         children: Optional[dict[str, KfpTask]],
#     ):
#         name = execution.custom_properties.get('task_name').string_value
#         iteration_index = execution.custom_properties.get('iteration_index')
#         if iteration_index:
#             name += f'-#{iteration_index.int_value}'
#         execution_type = execution_types_by_id[execution.type_id]
#         params = parse_parameters(execution)
#         events = events_by_execution_id.get(execution.id, [])
#         input_artifacts = []
#         output_artifacts = []
#         if events:
#             input_artifacts_info = [(e.artifact_id, e)
#                                     for e in events
#                                     if e.type == metadata_store_pb2.Event.INPUT]
#             output_artifacts_info = [
#                 (e.artifact_id, e)
#                 for e in events
#                 if e.type == metadata_store_pb2.Event.OUTPUT
#             ]

#             def kfp_artifact(aid: int,
#                              e: metadata_store_pb2.Event) -> KfpArtifact:
#                 mlmd_artifact = artifacts_by_id[aid]
#                 mlmd_type = artifact_types_by_id[mlmd_artifact.type_id]
#                 return KfpArtifact.new(
#                     mlmd_artifact=mlmd_artifact,
#                     mlmd_artifact_type=mlmd_type,
#                     mlmd_event=e,
#                 )

#             input_artifacts = [
#                 kfp_artifact(aid, e) for (aid, e) in input_artifacts_info
#             ]
#             input_artifacts.sort(key=lambda a: a.name)
#             output_artifacts = [
#                 kfp_artifact(aid, e) for (aid, e) in output_artifacts_info
#             ]
#             output_artifacts.sort(key=lambda a: a.name)

#         return cls(
#             name=name,
#             type=execution_type.name,
#             state=execution.last_known_state,
#             inputs=TaskInputs(
#                 parameters=params['inputs'], artifacts=input_artifacts),
#             outputs=TaskOutputs(
#                 parameters=params['outputs'], artifacts=output_artifacts),
#             children=children or None,
#         )


# class MlmdClient:

#     def __init__(
#         self,
#         mlmd_connection_config: Optional[
#             metadata_store_pb2.MetadataStoreClientConfig] = None,
#     ):
#         if mlmd_connection_config is None:
#             # default to value suitable for local testing
#             mlmd_connection_config = metadata_store_pb2.MetadataStoreClientConfig(
#                 host='localhost',
#                 port=8080,
#             )
#         self.mlmd_store = metadata_store.MetadataStore(mlmd_connection_config)
#         self.dag_type = self.mlmd_store.get_execution_type(
#             type_name='system.DAGExecution')

#     def get_tasks(self, run_id: str):
#         run_context = self.mlmd_store.get_context_by_type_and_name(
#             type_name='system.PipelineRun',
#             context_name=run_id,
#         )
#         if not run_context:
#             raise ValueError(
#                 f'Cannot find system.PipelineRun context "{run_id}"')
#         print(f'run_context: name={run_context.name} id={run_context.id}')

#         root = self.mlmd_store.get_execution_by_type_and_name(
#             type_name='system.DAGExecution',
#             execution_name=f'run/{run_id}',
#         )
#         if not root:
#             raise ValueError(
#                 f'Cannot find system.DAGExecution execution "run/{run_id}"')
#         print(f'root_dag: name={root.name} id={root.id}')
#         dag_id = root.id
#         run_context_id = run_context.id
#         return self._get_tasks(root.id, run_context.id)

#     def _get_tasks(self, dag_id: int,
#                    run_context_id: int) -> dict[str, KfpTask]:
#         filter_query = f'contexts_run.id = {run_context_id} AND custom_properties.parent_dag_id.int_value = {dag_id}'

#         executions = self.mlmd_store.get_executions(
#             list_options=ListOptions(filter_query=filter_query))

#         execution_types = self.mlmd_store.get_execution_types_by_id(
#             list({e.type_id for e in executions}))

#         execution_types_by_id = {et.id: et for et in execution_types}
#         events = self.mlmd_store.get_events_by_execution_ids(
#             [e.id for e in executions])

#         events_by_execution_id = {}
#         for e in events:
#             events_by_execution_id[e.execution_id] = (
#                 events_by_execution_id.get(e.execution_id) or []) + [e]

#         artifacts = self.mlmd_store.get_artifacts_by_id(
#             artifact_ids=[e.artifact_id for e in events])

#         artifacts_by_id = {a.id: a for a in artifacts}
#         artifact_types = self.mlmd_store.get_artifact_types_by_id(
#             list({a.type_id for a in artifacts}))

#         artifact_types_by_id = {at.id: at for at in artifact_types}
#         validate_executions_have_task_names(executions)

#         def get_children(e: Execution) -> Optional[dict[str, KfpTask]]:
#             if e.type_id == self.dag_type.id:
#                 children = self._get_tasks(e.id, run_context_id)
#                 return children
#             return None

#         tasks = [
#             KfpTask.new(
#                 execution=e,
#                 execution_types_by_id=execution_types_by_id,
#                 events_by_execution_id=events_by_execution_id,
#                 artifacts_by_id=artifacts_by_id,
#                 artifact_types_by_id=artifact_types_by_id,
#                 children=get_children(e)) for e in executions
#         ]

#         tasks_by_name = {t.name: t for t in tasks}
#         return tasks_by_name


# def validate_executions_have_task_names(execution_list):
#     executions_without_task_name = [
#         e for e in execution_list
#         if not e.custom_properties.get('task_name').string_value
#     ]
#     if executions_without_task_name:
#         raise ValueError(
#             f'some executions are missing task_name custom property. executions:\n{executions_without_task_name}'
#         )


# def parse_parameters(execution: metadata_store_pb2.Execution) -> dict:
#     custom_properties = execution.custom_properties
#     parameters = {'inputs': {}, 'outputs': {}}
#     for item in custom_properties.items():
#         (name, value) = item
#         raw_value = None
#         if value.HasField('string_value'):
#             raw_value = value.string_value
#         if value.HasField('int_value'):
#             raw_value = value.int_value
#         if value.HasField('double_value'):
#             raw_value = value.double_value
#         if name.startswith('input:'):
#             parameters['inputs'][name[len('input:'):]] = raw_value
#         if name.startswith('output:'):
#             parameters['outputs'][name[len('output:'):]] = raw_value
#         if name == 'inputs' and value.HasField('struct_value'):
#             for k, v in simplify_proto_struct(
#                     MessageToDict(value))['structValue'].items():
#                 parameters['inputs'][k] = v
#         if name == 'outputs' and value.HasField('struct_value'):
#             for k, v in simplify_proto_struct(
#                     MessageToDict(value))['structValue'].items():
#                 parameters['outputs'][k] = v
#     return parameters


# def simplify_proto_struct(data: dict) -> dict:
#     res = {}
#     for key, value in data.items():
#         if value.get('stringValue') is not None:
#             res[key] = value['stringValue']
#         elif value.get('doubleValue') is not None:
#             res[key] = value['doubleValue']
#         elif value.get('structValue') is not None:
#             res[key] = value['structValue']
#         else:
#             res[key] = value
#     return res

from ml_metadata.proto import metadata_store_pb2
from ml_metadata.metadata_store import metadata_store
print(metadata_store.MetadataStore)
# import inspect
# print(inspect.signature(metadata_store_pb2.ConnectionConfig))
# mlmd_conn_conf = metadata_store_pb2.ConnectionConfig(
#                 host='localhost',
#                 port=8080,
#             )
# print(mlmd_conn_conf)