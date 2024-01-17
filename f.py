from kfp import task
from kfp import entrypoint
import kfp

kfp.init(
    runner=kfp.CloudRunRunner(project='cjmccarthy-kfp', location='us-central1'))


@task
def identity(string: str) -> str:
    return string


@task
def identity(string: str) -> str:
    return string


@entrypoint
def my_pipeline(string: str = 'string'):
    op1 = identity(string=string)
    op2 = identity(string=op1.output)


my_pipeline()
