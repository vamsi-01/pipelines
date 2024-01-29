from kfp import dsl
from kfp import local
from kfp.dsl import *

local.init(local.SubprocessRunner())


@dsl.component
def square(x: float) -> float:
    return x**2


@dsl.component
def add(x: float, y: float) -> float:
    return x + y


@dsl.component
def square_root(x: float) -> float:
    return x**.5


@dsl.pipeline
def square_and_sum(a: float, b: float) -> float:
    a_sq_task = square(x=a)
    b_sq_task = square(x=b)
    return add(x=a_sq_task.output, y=b_sq_task.output).output


@dsl.pipeline
def pythagorean(a: float = 1.2, b: float = 1.2) -> float:
    sq_and_sum_task = square_and_sum(a=a, b=b)
    return square_root(x=sq_and_sum_task.output).output


# from kfp import dsl
# from kfp.dsl import *
# from typing import *

# @dsl.component
# def identity(string: str) -> str:
#     return string

# @dsl.pipeline
# def my_pipeline(string: str = 'string'):
#     op1 = identity(string=string)
#     op2 = identity(string=op1.output)

# if __name__ == '__main__':
#     import datetime
#     import warnings
#     import webbrowser

#     from google.cloud import aiplatform

#     from kfp import compiler

#     warnings.filterwarnings('ignore')
#     ir_file = __file__.replace('.py', '.yaml')
#     compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
print(square_and_sum(a=1.2, b=1.2).outputs)
print(pythagorean().outputs)

# if __name__ == '__main__':
#     import datetime
#     import warnings
#     import webbrowser

#     from google.cloud import aiplatform

#     from kfp import compiler

#     warnings.filterwarnings('ignore')
#     ir_file = __file__.replace('.py', '.yaml')
#     compiler.Compiler().compile(pipeline_func=pythagorean, package_path=ir_file)
#     pipeline_name = __file__.split('/')[-1].replace('_', '-').replace('.py', '')
#     display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
#     job_id = f'{pipeline_name}-{display_name}'
#     aiplatform.PipelineJob(
#         template_path=ir_file,
#         pipeline_root='gs://cjmccarthy-kfp-default-bucket',
#         display_name=pipeline_name,
#         job_id=job_id).submit()
#     url = f'https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/{pipeline_name}-{display_name}?project=271009669852'
#     webbrowser.open_new_tab(url)
