from kfp import dsl
from kfp import local
from kfp.dsl import *

local.init(
    runner=local.config.CloudRunRunner(
        project='cjmccarthy-kfp', location='us-central1'))


@dsl.component
def identity(string: str) -> str:
    return string


identity(string='foo')
