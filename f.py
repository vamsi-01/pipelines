from kfp import dsl


@dsl.component(kfp_package_path='git+https://github.com/connor-mccarthy/pipelines.git@add-logging-to-exector#subdirectory=sdk/python')
def identity(string: str) -> str:
    return string


@dsl.pipeline
def my_pipeline(string: str = 'string'):
    op1 = identity(string=string)
    op2 = identity(string=op1.output)

