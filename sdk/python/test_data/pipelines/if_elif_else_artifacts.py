from kfp import compiler
from kfp import dsl
from kfp.dsl import Artifact
from kfp.dsl import Output


@dsl.component
def flip_coin(a: Output[Artifact]):
    import random
    val = random.randint(0, 2)

    if val == 0:
        text = 'heads'
    elif val == 1:
        text = 'tails'
    else:
        text = 'draw'

    with open(a.path, 'w') as f:
        f.write(text)


@dsl.component
def print_and_return(text: str, a: Output[Artifact]):
    with open(a.path, 'w') as f:
        f.write(text)
    print(text)


@dsl.pipeline
def flip_coin_pipeline():
    flip_coin_task = flip_coin()
    with dsl.If(flip_coin_task.outputs['a'] == 'heads'):
        heads_task = print_and_return(text='Got heads!')
    with dsl.Elif(flip_coin_task.outputs['a'] == 'tails'):
        tails_task = print_and_return(text='Got tails!')
    with dsl.Else():
        draw_task = print_and_return(text='Draw!')


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=flip_coin_pipeline,
        package_path=__file__.replace('.py', '.yaml'))
