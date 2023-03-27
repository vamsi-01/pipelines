from kfp import dsl
from google_cloud_pipeline_components import aiplatform as gcc_aip

PIPELINE_ROOT = 'gs://cjmccarthy-kfp-default-bucket'
BUCKET_NAME = PIPELINE_ROOT
REGION = 'us-central1'
PROJECT_ID = '271009669852'


@dsl.pipeline(name="automl-beans-custom", pipeline_root=PIPELINE_ROOT)
def pipeline(bq_source: str = "bq://sara-vertex-demos.beans_demo.large_dataset",
             bucket: str = BUCKET_NAME,
             project: str = PROJECT_ID,
             gcp_region: str = REGION,
             bq_dest: str = "",
             container_uri: str = "",
             batch_destination: str = ""):
    dataset_create_op = gcc_aip.TabularDatasetCreateOp(
        display_name="tabular-beans-dataset",
        bq_source=bq_source,
        project=project,
        location=gcp_region)

    training_op = gcc_aip.CustomContainerTrainingJobRunOp(
        display_name="pipeline-beans-custom-train",
        container_uri=container_uri,
        project=project,
        location=gcp_region,
        dataset=dataset_create_op.outputs["dataset"],
        staging_bucket=bucket,
        training_fraction_split=0.8,
        validation_fraction_split=0.1,
        test_fraction_split=0.1,
        bigquery_destination=bq_dest,
        model_serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.0-24:latest",
        model_display_name="scikit-beans-model-pipeline",
        machine_type="n1-standard-4",
    )
    batch_predict_op = gcc_aip.ModelBatchPredictOp(
        project=project,
        location=gcp_region,
        job_display_name="beans-batch-predict",
        model=training_op.outputs["model"],
        gcs_source_uris=["{0}/batch_examples.csv".format(BUCKET_NAME)],
        instances_format="csv",
        gcs_destination_output_uri_prefix=batch_destination,
        machine_type="n1-standard-4")


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=pipeline, package_path=ir_file)
    pipeline_name = __file__.split('/')[-1].replace('_', '-').replace('.py', '')
    display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
    job_id = f'{pipeline_name}-{display_name}'
    aiplatform.PipelineJob(
        template_path=ir_file,
        pipeline_root='gs://cjmccarthy-kfp-default-bucket',
        display_name=pipeline_name,
        job_id=job_id).submit()
    url = f'https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/{pipeline_name}-{display_name}?project=271009669852'
    webbrowser.open_new_tab(url)