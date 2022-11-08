import kfp
from kfp import dsl
from kfp import gcp

kfp.use_platform_package(gcp)


@dsl.component
def identity(string: str) -> str:
    return string


@dsl.pipeline
def my_pipeline(string: str = 'string'):
    op1 = identity(string=string)
    op1.gcp.add_secret(secret='SECRET1', value='val')
    print(op1.platform_configuration)
