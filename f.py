import kfp

kfp.__version__ = '2.0.0-rc.1'
from kfp import dsl
from typing import List

# @dsl.component
# def str_to_list(string: str) -> List:
#     return [string]

# @dsl.component
# def identity(string: str) -> str:
#     return string

# @dsl.pipeline
# def my_pipeline():

#     # for-loop-2
#     with dsl.ParallelFor(['a', 'b', 'c']) as itema:
#         t1 = str_to_list(string=itema)
#         t2 = str_to_list(string=itema)

#         sequential_task1 = identity(string=itema)
#         identity(string=sequential_task1.output)


#         # # for-loop-3
#         # with dsl.ParallelFor(t1.output) as itemb:
#         #     t3 = str_to_list(string=itema)
#         #     with dsl.ParallelFor(t3.output) as itemc:
#         #         identity(string=itemc)
#         #     with dsl.ParallelFor(t2.output) as itemd:
#         #         identity(string=itemd)
#         # with dsl.ParallelFor(t2.output) as iteme:
#         #     identity(string=iteme)
@dsl.component
def print_op(message: str):
    print(message)


@dsl.pipeline(name='pipeline-with-multiple-exit-handlers')
def my_pipeline():
    first_exit_task = print_op(message='First exit task.')

    with dsl.ExitHandler(first_exit_task):
        first_print_op = print_op(message='Inside first exit handler.')

    second_exit_task = print_op(message='Second exit task.')
    with dsl.ExitHandler(second_exit_task):
        x = print_op(message='Inside second exit handler.')
        x.after(first_print_op)