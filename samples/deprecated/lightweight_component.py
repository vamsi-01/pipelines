import kfp.deprecated as kfp
import kfp.deprecated.components as components


#Define a Python function
def add(a: float, b: float) -> float:
    """Calculates sum of two arguments."""
    return a + b


add_op = components.create_component_from_func(add)

#Advanced function
#Demonstrates imports, helper functions and multiple outputs
from typing import NamedTuple


def my_divmod(
    dividend: float, divisor: float
) -> NamedTuple('MyDivmodOutput', [('quotient', float), ('remainder', float),
                                   ('mlpipeline_ui_metadata', 'UI_metadata'),
                                   ('mlpipeline_metrics', 'Metrics')]):
    """Divides two numbers and calculate the quotient and remainder."""

    #Imports inside a component function:
    import numpy as np

    #This function demonstrates how to use nested functions inside a component function:
    def divmod_helper(dividend, divisor):
        return np.divmod(dividend, divisor)

    (quotient, remainder) = divmod_helper(dividend, divisor)

    import json

    from tensorflow.python.lib.io import file_io

    # Exports a sample tensorboard:
    metadata = {
        'outputs': [{
            'type': 'tensorboard',
            'source': 'gs://ml-pipeline-dataset/tensorboard-train',
        }]
    }

    # Exports two sample metrics:
    metrics = {
        'metrics': [{
            'name': 'quotient',
            'numberValue': float(quotient),
        }, {
            'name': 'remainder',
            'numberValue': float(remainder),
        }]
    }

    from collections import namedtuple
    divmod_output = namedtuple('MyDivmodOutput', [
        'quotient', 'remainder', 'mlpipeline_ui_metadata', 'mlpipeline_metrics'
    ])
    return divmod_output(quotient, remainder, json.dumps(metadata),
                         json.dumps(metrics))


divmod_op = components.create_component_from_func(
    my_divmod, base_image='tensorflow/tensorflow:1.11.0-py3')

import kfp.deprecated.dsl as dsl


@dsl.pipeline(
    name='calculation-pipeline',
    description='A toy pipeline that performs arithmetic calculations.')
def calc_pipeline(
    a=7,
    b=8,
    c=17,
):
    #Passing pipeline parameter and a constant value as operation arguments
    add_task = add_op(a, 4)  #Returns a dsl.ContainerOp class instance.

    #Passing a task output reference as operation arguments
    #For an operation with a single return value, the output reference can be accessed using `task.output` or `task.outputs['output_name']` syntax
    divmod_task = divmod_op(add_task.output, b)

    #For an operation with a multiple return values, the output references can be accessed using `task.outputs['output_name']` syntax
    result_task = add_op(divmod_task.outputs['quotient'], c)


if __name__ == '__main__':
    #Specify pipeline argument values
    arguments = {'a': 7, 'b': 8}

    #Submit a pipeline run
    kfp.Client().create_run_from_pipeline_func(
        calc_pipeline, arguments=arguments)

    # Run the pipeline on a separate Kubeflow Cluster instead
    # (use if your notebook is not running in Kubeflow - e.x. if using AI Platform Notebooks)
    # kfp.Client(host='<ADD KFP ENDPOINT HERE>').create_run_from_pipeline_func(calc_pipeline, arguments=arguments)

    #vvvvvvvvv This link leads to the run information page. (Note: There is a bug in JupyterLab that modifies the URL and makes the link stop working)
