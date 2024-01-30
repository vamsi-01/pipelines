import io
import re

ops = {
    '==': lambda a, b: a == b,
    '!=': lambda a, b: a != b,
    '<': lambda a, b: a < b,
    '<=': lambda a, b: a <= b,
    '>': lambda a, b: a > b,
    '>=': lambda a, b: a >= b,
}


def evaluate_condition(
    condition: str,
    io_store: io.IOStore,
) -> bool:
    for op, fn in ops.items():
        split = condition.split(f' {op} ')
        if len(split) == 1:
            continue
        elif len(split) > 2:
            # user should not hit this
            raise ValueError(f"Got invalid condition: {condition}")
        else:
            break
    left, right = split
    key = extract_key_from_condition(condition=condition)
    placeholder_utils.get_value_using_path()


def extract_key_from_condition(condition: str) -> str:
    pattern = r"\['(.*?)'\]"
    match = re.search(pattern, condition)
    if not match:
        raise ValueError(
            f'Could not find DAG input key in condition: {condition}')
    return match[1]
