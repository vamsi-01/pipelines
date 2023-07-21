from kfp import compiler
from kfp import dsl


@dsl.component
def flip_coin() -> str:
    import random
    return 'heads' if random.randint(0, 1) == 0 else 'tails'


@dsl.component
def print_and_return(text: str) -> str:
    print(text)
    return text


@dsl.pipeline
def flip_coin_pipeline() -> str:
    flip_coin_task = flip_coin()
    with dsl.If(flip_coin_task.output == 'heads'):
        heads_task = print_and_return(text='Got heads!')
    with dsl.Else():
        tails_task = print_and_return(text='Got tails!')

    oneof = dsl.OneOf(
        heads_task.output,
        tails_task.output,
    )
    print_and_return(text=oneof)

    return oneof


if __name__ == '__main__':
    compiler.Compiler().compile(
        pipeline_func=flip_coin_pipeline,
        package_path=__file__.replace('.py', '.yaml'))
