import typing

from kfp import dsl
from kfp.dsl import Dataset
from kfp.dsl import Input
from kfp.dsl import Model
from kfp.dsl import Output


@dsl.component
def make_dataset(text: str, dataset: Output[Dataset]):
    with open(dataset.path, 'w') as f:
        f.write(text)


@dsl.component
def train_model(epochs: int, dataset: Input[Dataset], model: Output[Model]):
    with open(dataset.path) as f:
        data = f.read()
        # do training and evaluation
        # save model to path
        f.write('dummy_model')


@dsl.pipeline
def outer_make_dataset(text: str) -> Dataset:
    return make_dataset(text=text).outputs['dataset']


@dsl.pipeline
def outer_train_model(
    epochs: int, dataset: Input[Dataset]
) -> typing.NamedTuple('Outputs', [
    ('epochs', int),
    ('model', Model),
]):
    # outputs = typing.NamedTuple('Outputs', [
    #     ('epochs', int),
    #     ('model', Model),
    # ])
    return outputs(
        epochs=epochs,
        model=train_model(epochs=epochs, dataset=dataset).outputs['model'],
    )


@dsl.pipeline
def training_workflow():
    make_dataset_op = outer_make_dataset(text='my_dataset')
    outer_train_model(epochs=10, dataset=make_dataset_op.output)


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from google.cloud import aiplatform
    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(
        pipeline_func=training_workflow, package_path=ir_file)
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
