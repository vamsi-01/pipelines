def use_secret_as_env(
    task: PipelineTask,
    secret_name: str,
    secret_key_to_env: Dict[str, str],
) -> None:
    """Use a Kubernetes Secret as an environment variable as described in https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-environment-variables.

    Args:
        task: Pipeline task.
        secret_name: Name of the Secret.
        secret_key_to_env: Map of Secret key's to environment variable names. E.g., {'password': 'PASSWORD'} maps the Secret's password value to 
            the environment variable PASSWORD.
    """


def use_secret_as_volume(
    task: PipelineTask,
    secret_name: str,
    mount_path: str,
) -> None:
    """Use a Kubernetes Secret by mounting its data to the task's Pod as described in https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod.

    Args:
        task: Pipeline task.
        secret_name: Name of the Secret.
        mount_path: Path to which to mount the Secret data.
    """


def mount_pvc(
    task: PipelineTask,
    access_modes: List[str],
    size: str,
    mount_path: str,
    pvc_name: Optional[str] = None,
    pvc_name_suffix: Optional[str] = None,
    storage_class: Optional[str] = None,
    volume_name: Optional[str] = None,
    annotations: Optional[Dict[str, str]] = None,
) -> str:
    """Mount a PersistentVolumeClaim (PVC) to the task container. Possibly use an existing PersistentVolume or PersistentVolumeClaim, depending on provided parameters.

    Args:
        task: Pipeline task.
        access_modes: AccessModes to request for the provisioned PVC. May multiple
        of ReadWriteOnce, ReadOnlyMany, ReadWriteMany, or ReadWriteOncePod.
        size: The size of storage requested by the PVC that will be provisioned.
        mount_path: Path within the container at which the volume should be mounted.
        pvc_name: Name to use for the provisioned PVC. Only one of generate_pvc_name
            or pvc_name can be provided. You may also pass the output of another
            mount_pvc call to this parameter to specify that two tasks should use
            the same PVC.
        pvc_name_suffix: Suffix of name to use for the provisioned PVC. Name takes the
            form {{workflow.name}}-<pv_name_suffix> where {{workflow.name}} is the Argo
            workflow name. Used for a dynamically provisioned PV only. Only one of
            generate_pvc_name or pvc_name can be provided.
        storage_class: Name of StorageClass from which to provision the PV to back
            the PVC. Used for dynamically provisioned PV only.
        volume_name: Pre-existing PersistentVolume that should back the provisioned
            PersistentVolumeClaim. Used for statically specified PV only.
        annotations: Annotations for the PVC's metadata.
    
    Returns:
        str: Reference to the name of the PVC created. Can be passed to the
            pvc_name argument of other .mount_pvc calls to reuse the same PVC
            for multiple tasks. This is a reference and is not inspectable at
            compile-time.
    """

    ...