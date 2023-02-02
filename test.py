import kfp
from kfp import compiler
from kfp import components

print(kfp.__version__)

bq_comp = components.load_component_from_url(
    'https://raw.githubusercontent.com/kubeflow/pipelines/google-cloud-pipeline-components-1.0.31/components/google-cloud/google_cloud_pipeline_components/v1/bigquery/query_job/component.yaml'
)

compiler.Compiler().compile(bq_comp, 'pipeline.yaml')
