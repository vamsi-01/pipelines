from kfp import dsl


@dsl.container_component
def echo_bool(boolean: bool = True):
    return dsl.ContainerSpec(
        image='alpine', command=['sh', '-c', f'echo {boolean}'])


print(echo_bool.component_spec.implementation.container)

# @dsl.pipeline
# def my_pipeline(string: str = 'string'):
#     op1 = echo_bool()

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
