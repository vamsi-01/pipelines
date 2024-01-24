from kfp import dsl
from kfp import local
from kfp.dsl import *

local.init(runner=local.SubprocessRunner())


@dsl.component
def printer(a: Artifact):
    print(a)


@dsl.component
def identity(string: str) -> str:
    return string


@dsl.pipeline
def my_pipeline():
    op0 = identity(string='foo')
    op1 = identity(string='/artifact/bar',)
    op2 = dsl.importer(
        artifact_uri=op1.output,
        artifact_class=Artifact,
        metadata={
            op0.output: 1,
            op1.output: op0.output
        })
    op2 = printer(a=op2.output)


# TODO: test case: uri string
# TODO: test case: uri from upsteam
my_pipeline()

# if __name__ == '__main__':
#     import datetime
#     import warnings
#     import webbrowser

#     from google.cloud import aiplatform

#     from kfp import compiler

#     warnings.filterwarnings('ignore')
#     ir_file = __file__.replace('.py', '.yaml')
#     compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
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
