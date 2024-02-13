from kfp import dsl
from kfp.dsl import *
from typing import *


@dsl.container_component
def say_hello(name: str, greeting: dsl.OutputPath(str)):
    """Log a greeting and return it as an output."""

    return dsl.ContainerSpec(
        image='alpine',
        command=[
            'sh', '-c', '''RESPONSE="Hello, $0!"\
                            && echo $RESPONSE\
                            && mkdir -p $(dirname $1)\
                            && echo $RESPONSE > $1
                            '''
        ],
        args=[name, greeting])


@dsl.component(base_image="python:3.12")
def print_text(text: dsl.InputPath(str)):
    print(text)


@dsl.pipeline
def my_pipeline(person_to_greet: str = 'Alice') -> None:
    hello_task = say_hello(name=person_to_greet)
    print_text(text=hello_task.outputs["greeting"])


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
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
