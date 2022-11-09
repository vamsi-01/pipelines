from kfp import dsl
from kfp.dsl import Artifact


@dsl.pipeline
def pipeline():
    with dsl.ParallelFor([1, 2, 3]) as num1:
        with dsl.ParallelFor([4, 5, 6]) as num2:
            task = add_producer(num1=num1, num2=num2)

    consumer(sums=task.outputs)

#     [(1, 4), (1, 5), (1, 6), (2, 4)]

outputs = []
for num1 in [1,2, 3]:
    for num2 [4, 5, 6]:
        outputs.append(num1 + num2)
