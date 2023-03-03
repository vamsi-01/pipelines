from google.protobuf import json_format
from kfp.dsl import PipelineTask
from typing import Dict
from kfp.kubernetes import kubernetes_executor_config_pb2 as pb


def use_secret_as_env(
    task: PipelineTask,
    secret_name: str,
    secret_key_to_env: Dict[str, str],
) -> None:
    """Use a Kubernetes Secret as an environment variable as described in https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-environment-variables.

    Args:
        task: The pipeline task.
        secret_name: The name of the Secret.
        secret_key_to_env: A map of Secret key's to environment variable names. E.g., {'password': 'PASSWORD'} maps the Secret's password value to the environment variable PASSWORD.
    """
    key_to_env = [
        pb.SecretAsEnv.SecretKeyToEnvMap(
            secret_key=secret_key,
            env_var=env_var,
        ) for secret_key, env_var in secret_key_to_env.items()
    ]
    secret_as_env = pb.SecretAsEnv(
        secret_name=secret_name,
        key_to_env=key_to_env,
    )

    # TODO
    task.platform_config['kubernetes'] = json_format.MessageToDict(
        pb.KubernetesExecutorConfig(secret_as_env=[secret_as_env]))


def use_secret_as_volume(
    task: PipelineTask,
    secret_name: str,
    mount_path: str,
) -> None:
    """Use a Kubernetes Secret by mounting its data to the task's container as described in https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod.

    Args:
        task: The pipeline task.
        secret_name: The name of the Secret.
        mount_path: The path to which to mount the Secret data.
    """
    secret_as_vol = pb.SecretAsVolume(
        secret_name=secret_name,
        mount_path=mount_path,
    )

    # TODO
    task.platform_config['kubernetes'] = json_format.MessageToDict(
        pb.KubernetesExecutorConfig(secret_as_vol=[secret_as_vol]))
