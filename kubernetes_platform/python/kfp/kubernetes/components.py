from kfp import dsl
from typing import Optional, List, Dict
from kfp.kubernetes import kubernetes_executor_config_pb2 as pb
from google.protobuf import json_format, struct_pb2


@dsl.container_component
def CreatePVC(
    name: dsl.OutputPath(str),
    access_modes: List[str],
    size: str,
    pvc_name: Optional[str] = None,
    pvc_name_suffix: Optional[str] = None,
    storage_class: Optional[str] = '',
    volume_name: Optional[str] = None,
    annotations: Optional[Dict[str, str]] = None,
):
    """Create a PersistentVolumeClaim, which can be used by downstream
        tasks.

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
    create_pvc = pb.CreatePvc()

    create_pvc.access_modes.extend(access_modes)
    create_pvc.size = size

    if bool(pvc_name) == bool(pvc_name_suffix):
        raise ValueError(
            f"Only one of arguments {'pvc_name'!r} or {'pvc_name_suffix'!r} permitted. Got {'both' if pvc_name else 'neither.'}"
        )
    if pvc_name:
        create_pvc.pvc_name = pvc_name
    else:
        create_pvc.pvc_name_suffix = pvc_name_suffix

    create_pvc.storage_class = storage_class
    create_pvc.volume_name = volume_name or ''

    create_pvc.annotations = struct_pb2.Struct()
    create_pvc.annotations.update(annotations or {})

    return dsl.ContainerSpec(
        image='argostub/createpvc',
        command=['echo'],
        args=[json_format.MessageToDict(create_pvc)],
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
#     from google.protobuf import json_format

#     pvc_mount = PvcMount(mount_path=mount_path)
#     if isinstance(pvc_name, str):
#         pvc_mount.pvc_name = pvc_name
#     elif hasattr(pvc_name, 'task_name') and pvc_name.task_name is not None:
#         pvc_mount.task_output_parameter.producer_task = pvc_name.task_name
#         pvc_mount.task_output_parameter.output_parameter_key = pvc_name.name
#     elif hasattr(pvc_name, 'task_name') and pvc_name.task_name is None:
#         pvc_mount.component_input_parameter = pvc_name.name
#     else:
#         raise ValueError(
#             f'Got unknown input type for pvc_name: {type(pvc_name)!r}. Input type must be an instance of str or PipelineChannel.'
#         )

#     # ... logic to check if a kubernetes platform config already exists...
#     task.platform_config['kubernetes'] = json_format.MessageToDict(
#         KubernetesExecutorConfig(pvc_mount=[pvc_mount]))

#     return task


@dsl.container_component
def DeletePVC(pvc_name: str):
    """Delete a PersistentVolumeClaim.

    Args:
        pvc_name: Name of the PVC to delete. Can also be a runtime-generated name
            reference provided by kubernetes.CreatePvcOp().outputs['pvc_name'].
    """
    delete_pvc = pb.DeletePvc()
    if isinstance(pvc_name, str):
        delete_pvc.pvc_name = pvc_name
    elif hasattr(pvc_name, 'task_name') and pvc_name.task_name is not None:
        delete_pvc.task_output_parameter.producer_task = pvc_name.task_name
        pvc_mount.task_output_parameter.output_parameter_key = pvc_name.name
    elif hasattr(pvc_name, 'task_name') and pvc_name.task_name is None:
        pvc_mount.component_input_parameter = pvc_name.name
    else:
    return dsl.ContainerSpec(
        image='argostub/deletepvc',
        command=['echo'],
        args=[json_format.MessageToDict(delete_pvc)],
    )