from typing import Dict, List, Optional, Union

from google.protobuf import json_format
from google.protobuf import struct_pb2
from kfp import dsl
from kfp.dsl import PipelineTask
from kfp.kubernetes import kubernetes_executor_config_pb2 as pb
from google.protobuf import json_format, message


@dsl.container_component
def CreatePVC(
    name: dsl.OutputPath(str),
    access_modes: List[str],
    size: str,
    pvc_name: Optional[str] = None,
    pvc_name_suffix: Optional[str] = None,
    storage_class: Optional[str] = '',
    volume_name: Optional[str] = None,
    # annotations: Optional[Dict[str, str]] = None,
):
    """Create a PersistentVolumeClaim, which can be used by downstream tasks.

    Args:
        access_modes: AccessModes to request for the provisioned PVC. May
            be multiple of ReadWriteOnce, ReadOnlyMany, ReadWriteMany, or
            ReadWriteOncePod.
        size: The size of storage requested by the PVC that will be provisioned.
        pvc_name: Name of the PVC. Only one of pvc_name and pvc_name_suffix can
            be provided.
        pvc_name_suffix: Prefix to use for a dynamically generated name, which
            will take the form <argo-workflow-name>-<pvc_name_suffix>. Only one
            of pvc_name and pvc_name_suffix can be provided.
        storage_class: Name of StorageClass from which to provision the PV
            to back the PVC. `None` indicates to use the cluster's default
            storage_class. Set to `''` for a statically specified PVC.
        volume_name: Pre-existing PersistentVolume that should back the
            provisioned PersistentVolumeClaim. Used for statically
            specified PV only.
        annotations: Annotations for the PVC's metadata.

    Outputs:
        name: The name of the generated PVC.
    """
    create_pvc = dict(
        access_modes=str(access_modes),
        size=str(size),
        storage_class=str(storage_class),
        volume_name=str(volume_name or ''),
    )
    # pb.CreatePvc(
    #     access_modes=access_modes,
    # size=size,
    # storage_class=storage_class,
    # volume_name=volume_name or '',
    # )

    if bool(pvc_name) == bool(pvc_name_suffix):
        raise ValueError(
            f"Only one of arguments {'pvc_name'!r} or {'pvc_name_suffix'!r} permitted. Got {'both' if pvc_name else 'neither'}."
        )
    if pvc_name:
        create_pvc['pvc_name'] = str(pvc_name)
    else:
        create_pvc['pvc_name_suffix'] = str(pvc_name_suffix)

    # create_pvc.annotations.CopyFrom(struct_pb2.Struct())
    # create_pvc.annotations.update(annotations or {})
    # print({k: str(v) for k, v in create_pvc.items()})

    import json
    return dsl.ContainerSpec(
        image='argostub/createpvc',
        command=['echo'],
        args=[json.dumps(create_pvc)],
    )


# def mount_pvc(
#     task: PipelineTask,
#     pvc_name: str,
#     mount_path: str,
# ) -> None:
#     """Mount a PersistentVolumeClaim to the task container.

#     Args:
#         task: Pipeline task.
#         pvc_name: Name of the PVC to mount. Can also be a runtime-generated name
#             reference provided by kubernetes.CreatePvcOp().outputs['pvc_name'].
#         mount_path: Path to which the PVC should be mounted as a volume.
#     """

#     pvc_mount = pb.PvcMount(mount_path=mount_path)
#     _assign_pvc_name_to_msg(pvc_mount, pvc_name)
#     pvc_mount.mount_path = mount_path
#     # TODO
#     task.platform_config['kubernetes'] = json_format.MessageToDict(
#         pb.KubernetesExecutorConfig(pvc_mount=[pvc_mount]))

#     return task

# @dsl.container_component
# def DeletePVC(pvc_name: str):
#     """Delete a PersistentVolumeClaim.

#     Args:
#         pvc_name: Name of the PVC to delete. Can also be a runtime-generated name
#             reference provided by kubernetes.CreatePvcOp().outputs['name'].
#     """
#     delete_pvc = pb.DeletePvc()
#     _assign_pvc_name_to_msg(delete_pvc, pvc_name)

#     return dsl.ContainerSpec(
#         image='argostub/deletepvc',
#         command=['echo'],
#         args=[json_format.MessageToDict(delete_pvc)],
#     )

# def _assign_pvc_name_to_msg(msg: message.Message,
#                             pvc_name: Union[str, 'PipelineChannel']) -> None:
#     if isinstance(pvc_name, str):
#         msg.pvc_name = pvc_name
#     elif hasattr(pvc_name, 'task_name'):
#         if pvc_name.task_name is None:
#             msg.component_input_parameter = pvc_name.name
#         else:
#             msg.task_output_parameter.producer_task = pvc_name.task_name
#             msg.task_output_parameter.output_parameter_key = pvc_name.name
#     raise ValueError(
#         f'Argument for {"pvc_name"!r} must be an instance of str or PipelineChannel. Got unknown input type: {type(pvc_name)!r}. '
#     )
