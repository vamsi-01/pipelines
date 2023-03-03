from google_cloud_pipeline_components.experimental.vertex_notification_email import VertexNotificationEmailOp

from kfp import dsl
from typing import List


@dsl.component
def hey():
    print('hey')


@dsl.pipeline
def x():
    x = VertexNotificationEmailOp(recipients=['hi', 'hi'])
    with dsl.ExitHandler(x):
        hey()


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=x, package_path=ir_file)
