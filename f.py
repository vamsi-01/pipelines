from kfp import dsl
import kfp

import kfp.kubernetes

kfp.register_platform(kfp.kubernetes.KubernetesPlatform)


@dsl.component
def identity(string: str) -> str:
    return string


@dsl.pipeline
def my_pipeline(string: str = 'string'):
    op1 = identity(string=string)
    op1.kubernetes.add_pod_label(
        label_key='key',
        label_value='value',
    )


import google_cloud_pipeline_components as vertex_pipelines
# from google.cloud.aiplatform import pipelines as vertex_pipelines
# import kfp.vertex as vertex_pipelines

kfp.register_platform(vertex_pipelines)


@dsl.pipeline
def my_pipeline(string: str = 'string'):
    op1 = identity(string=string)
    op1.vertex.set_service_account(service_account='compute-12345@google.com')
    op1.vertex.set_worker_pool_specs(...)
