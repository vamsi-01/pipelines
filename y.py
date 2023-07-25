from kfp import dsl


@dsl.component(
    kfp_package_path='git+https://github.com/kubeflow/pipelines.git@master#subdirectory=sdk/python',
)
def print_and_return(text: str) -> str:
    print(text)
    return text


@dsl.component(
    kfp_package_path='git+https://github.com/kubeflow/pipelines.git@master#subdirectory=sdk/python',
)
def flip_coin() -> str:
    return 'heads'


@dsl.pipeline
def flip_coin_pipeline():
    flip_coin_task = flip_coin()
    with dsl.If((flip_coin_task.output == 'heads')
                & (flip_coin_task.output == 'heads')):
        heads_task = print_and_return(text='Got heads!')


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(
        pipeline_func=flip_coin_pipeline, package_path=ir_file)
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