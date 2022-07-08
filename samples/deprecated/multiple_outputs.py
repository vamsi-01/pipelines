from typing import NamedTuple

import kfp.deprecated.components as components
import kfp.deprecated.dsl as dsl


@components.create_component_from_func
def product_sum(
        a: float,
        b: float) -> NamedTuple('output', [('product', float), ('sum', float)]):
    """Returns the product and sum of two numbers."""
    from collections import namedtuple

    product_sum_output = namedtuple('output', ['product', 'sum'])
    return product_sum_output(a * b, a + b)


@dsl.pipeline(
    name='multiple-outputs-pipeline',
    description='Sample pipeline to showcase multiple outputs')
def pipeline(a: float = 2.0, b: float = 2.5, c: float = 3.0):
    prod_sum_task = product_sum(a, b)
    prod_sum_task2 = product_sum(b, c)
    prod_sum_task3 = product_sum(prod_sum_task.outputs['product'],
                                 prod_sum_task2.outputs['sum'])
