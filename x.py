from kfp import dsl
from kfp.dsl import *
from typing import *


@dsl.container_component
def comp(
        str_in: str,
        int_in: int,
        float_in: float,
        bool_in: bool,
        dict_in: dict,
        list_in: list,
        str_out: dsl.OutputPath(str),
        int_out: dsl.OutputPath(int),
        float_out: dsl.OutputPath(float),
        bool_out: dsl.OutputPath(bool),
        dict_out: dsl.OutputPath(dict),
        list_out: dsl.OutputPath(list),
):
    return dsl.ContainerSpec(
        image='alpine',
        command=[
            'sh',
            '-c',
        ],
        args=[
            f'mkdir -p $(dirname {str_out}) && echo {str_in} > {str_out} &&'
            f'mkdir -p $(dirname {int_out}) && echo "{int_in}" > {int_out} &&'
            f'mkdir -p $(dirname {float_out}) && echo "{float_in}" > {float_out} &&'
            f'mkdir -p $(dirname {bool_out}) && echo "true" > {bool_out} && '
            f'mkdir -p $(dirname {dict_out}) && echo "{{}}"  > {dict_out} && '
            f'mkdir -p $(dirname {list_out}) && echo \'["a", "b", "c"]\' > {list_out}'
        ],
    )


@dsl.pipeline
def p():
    t = comp(
        str_in='foo',
        int_in=100,
        float_in=2.718,
        bool_in=False,
        dict_in={'x': 'y'},
        list_in=['a', 'b', 'c'])
    comp(
        str_in=t.outputs['str_out'],
        int_in=t.outputs['int_out'],
        float_in=t.outputs['float_out'],
        bool_in=t.outputs['bool_out'],
        dict_in=t.outputs['dict_out'],
        list_in=t.outputs['list_out'])


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=p, package_path=ir_file)
    pipeline_name = __file__.split('/')[-1].replace('_', '-').replace('.py', '')
    display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
    job_id = f'{pipeline_name}-{display_name}'
    aiplatform.PipelineJob(
        template_path=ir_file,
        pipeline_root='gs://cjmccarthy-kfp-default-bucket',
        display_name=pipeline_name,
        job_id=job_id).submit()
    url = f'https://console.cloud.google.com/vertex-ai/locations/us-central1/pipelines/runs/{pipeline_name}-{display_name}?project=271009669852'
    webbrowser.open_new_tab(url)
