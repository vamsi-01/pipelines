import warnings
import json
import yaml


def _write_ir_to_file(ir_json: str, output_file: str) -> None:
    """Writes the IR JSON to a file.

    Args:
        ir_json (str): IR JSON.
        output_file (str): Output file path.

    Raises:
        ValueError: If output file path is not JSON or YAML.
    """

    if output_file.endswith(".json"):
        warnings.warn(
            ("Compiling to JSON is deprecated and will be "
             "removed in a future version. Please compile to a YAML file by "
             "providing a file path with .yaml extension instead."),
            category=DeprecationWarning,
            stacklevel=2,
        )
        with open(output_file, 'w') as json_file:
            json_file.write(ir_json)
    elif output_file.endswith((".yaml", ".yml")):
        json_dict = json.loads(ir_json)
        with open(output_file, 'w') as yaml_file:
            yaml.dump(json_dict, yaml_file, sort_keys=True)
    else:
        raise ValueError(
            f'The output path {output_file} should end with ".yaml".')