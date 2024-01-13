from kfp import dsl
from kfp.dsl import *
from typing import *


@dsl.component
def identity(string: str) -> str:
    return string


@dsl.pipeline
def my_pipeline(string: str = 'string'):
    op1 = identity(string=string)
    op2 = identity(string=op1.output)


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from kfp import compiler

    from kfp import client

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
    pipeline_name = __file__.split('/')[-1].replace('_', '-').replace('.py', '')
    display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
    job_id = f'{pipeline_name}-{display_name}'
    endpoint = 'https://3ef69444e0dc251d-dot-us-central1.pipelines.googleusercontent.com'
    kfp_client = client.Client(host=endpoint)
    run = kfp_client.create_run_from_pipeline_package(ir_file)
    url = f'{endpoint}/#/runs/details/{run.run_id}'
    print(url)
    webbrowser.open_new_tab(url)
