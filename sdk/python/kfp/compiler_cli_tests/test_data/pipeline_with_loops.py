# pylint: disable=all
from re import T
from typing import Dict, List

from kfp import compiler
from kfp import dsl


@dsl.component
def print_str(v: str) -> str:
    print(v)
    return v


@dsl.component
def print_int(v: int) -> int:
    print(v)
    return v


@dsl.component
def print_float(v: float) -> float:
    print(v)
    return float(v)


@dsl.component
def concat_op(a: str, b: str) -> str:
    print(a + b)
    return a + b


@dsl.component
def arg_generator_op() -> List[Dict[str, str]]:
    return [{'a': '1', 'b': '2'}, {'a': '10', 'b': '20'}]


@dsl.pipeline(name='pipeline-with-loop-static')
def my_pipeline(
    greeting: str = 'this is a test for looping through parameters',):
    dict_loop_args = [{'a': '1', 'b': '2'}, {'a': '10', 'b': '20'}]
    with dsl.ParallelFor(dict_loop_args, "static") as item:
        print_task = concat_op(a=item.a, b=item.b)

    generated_args = arg_generator_op()
    with dsl.ParallelFor(generated_args.output) as dynamic_item:
        with dsl.Condition(dynamic_item.a == '1', name="cond2"):
            print_str(v=dynamic_item.b)
    # dict_loop_args = [{'a': '1', 'b': '2'}, {'a': '10', 'b': '20'}]
    # with dsl.ParallelFor(dict_loop_args, "static") as item:
    #     print_task = concat_op(a=item.a, b=item.b)

    #     generated_args = arg_generator_op()
    #     with dsl.ParallelFor(generated_args.output, "dynamic") as gen_item:
    #         print_task = concat_op(a=gen_item.a, b=gen_item.b)

    # with dsl.ParallelFor(dict_loop_args, "mixed_static_outer") as item:
    #     print_task = concat_op(a=item.a, b=item.b)

    #     generated_args = arg_generator_op()
    #     with dsl.ParallelFor(generated_args, "mixed_dynamic_inner") as gen_item:
    #         print_task = concat_op(a=gen_item.a, b=item.b)

    # primitive element test cases to ensure more doesn't break
    # string_loop_args = ["a", "b"]
    # with dsl.ParallelFor(string_loop_args) as item:
    #     print_task = print_str(v=item)

    # int_loop_args = [1, 1]
    # with dsl.ParallelFor(int_loop_args, "int") as item:
    #     print_task = print_int(v=item)

    # float_loop_args = [1.0, 1.0]
    # with dsl.ParallelFor(float_loop_args, "float") as item:
    #     print_task = print_float(v=item)


if __name__ == '__main__':
    import datetime

    from google.cloud import aiplatform

    ir_file = __file__.replace('.py', '.json')
    compiler.Compiler().compile(
        pipeline_func=my_pipeline, package_path=ir_file, type_check=True)
    print(ir_file)
    aiplatform.PipelineJob(
        template_path=ir_file,
        pipeline_root='gs://cjmccarthy-kfp-default-bucket',
        display_name=str(datetime.datetime.now()),
        enable_caching=False).submit()
