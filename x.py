from kfp import dsl


@dsl.component
def comp(inner_region: str):
    print(inner_region)


@dsl.pipeline(name='graph')
def graph_component(middle_region: str):
    _ = comp(inner_region=middle_region)


@dsl.pipeline(name='test-pipeline')
def pipeline(outer_region: str = 'us-central1'):
    _ = graph_component(
        middle_region=f'{outer_region}-aiplatform.googleapis.com')


# _ = graph_component(regional_uri=region)

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