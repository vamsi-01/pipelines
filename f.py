import kfp


@kfp.dsl.component
def identity(string: str) -> str:
    return string


@kfp.dsl.pipeline
def my_pipeline(string: str = 'string'):
    identity(string=string)
