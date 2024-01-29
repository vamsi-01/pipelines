from kfp import dsl
from kfp import local

local.init(local.SubprocessRunner())


@dsl.component
def identity(string: str) -> str:
    return string


@dsl.pipeline
def inner_pipeline():
    identity(string='foo')


@dsl.pipeline
def my_pipeline():
    inner_pipeline()


my_pipeline()

if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
