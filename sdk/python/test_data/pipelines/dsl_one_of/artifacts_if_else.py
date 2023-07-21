from kfp import compiler
from kfp import dsl
from kfp.dsl import Artifact
from kfp.dsl import Output


@dsl.component
def flip_coin(a: Output[Artifact]):
    import random

    val = 'heads' if random.randint(0, 1) == 0 else 'tails'
    with open(a.path, 'w') as f:
        f.write(val)


@dsl.component
def print_and_return(text: str, a: Output[Artifact]):
    with open(a.path, 'w') as f:
        f.write(text)
    print(text)


@dsl.pipeline
def flip_coin_pipeline() -> str:
    flip_coin_task = flip_coin()
    with dsl.If(flip_coin_task.outputs['a'] == 'heads'):
        heads_task = print_and_return(text='Got heads!')
    with dsl.Else():
        tails_task = print_and_return(text='Got tails!')

    oneof = dsl.OneOf(
        heads_task.outputs['a'],
        tails_task.output['a'],
    )
    print_and_return(text=oneof)

    return oneof


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=flip_coin_pipeline,
        package_path=__file__.replace('.py', '.yaml'))
