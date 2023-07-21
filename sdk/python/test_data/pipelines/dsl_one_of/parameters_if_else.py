from kfp import compiler
from kfp import dsl


def get_path():
    return 'git+https://github.com/kubeflow/pipelines.git@master#subdirectory=sdk/python'


from kfp.dsl import component_factory

component_factory._get_default_kfp_package_path = get_path


@dsl.component
def flip_coin() -> str:
    import random
    return 'heads' if random.randint(0, 1) == 0 else 'tails'


@dsl.component
def print_and_return(text: str) -> str:
    print(text)
    return text


@dsl.pipeline
def flip_coin_pipeline():
    flip_coin_task = flip_coin()
    with dsl.If(flip_coin_task.output == 'heads'):
        heads_task = print_and_return(text='Got heads!')
    with dsl.Else():
        tails_task = print_and_return(text='Got tails!')

    oneof = dsl.OneOf(
        heads_task.output,
        tails_task.output,
    )
    # print_and_return(text=oneof)

    # return oneof


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