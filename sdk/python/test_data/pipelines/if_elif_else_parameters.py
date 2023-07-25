from kfp import compiler
from kfp import dsl


@dsl.component
def flip_coin() -> str:
    import random
    val = random.randint(0, 2)

    if val == 0:
        return 'heads'
    elif val == 1:
        return 'tails'
    else:
        return 'draw'


@dsl.component
def print_and_return(text: str) -> str:
    print(text)
    return text


@dsl.pipeline
def flip_coin_pipeline():
    flip_coin_task = flip_coin()
    with dsl.If(flip_coin_task.output == 'heads'):
        heads_task = print_and_return(text='Got heads!')
    with dsl.Elif(flip_coin_task.output == 'tails'):
        tails_task = print_and_return(text='Got tails!')
    with dsl.Else():
        draw_task = print_and_return(text='Draw!')


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=flip_coin_pipeline,
        package_path=__file__.replace('.py', '.yaml'))
