from google.protobuf import json_format
from kfp.pipeline_spec import pipeline_spec_pb2

dos = pipeline_spec_pb2.DagOutputsSpec()
dos.parameters['key'].value_from_oneof.parameter_selectors.add()
dos.parameters['key'].value_from_oneof.parameter_selectors[
    0].producer_subtask = 'hi'
dos.parameters['key'].value_from_oneof.parameter_selectors[
    0].output_parameter_key = '1'
dos.parameters['key'].value_from_oneof.parameter_selectors.add()
dos.parameters['key'].value_from_oneof.parameter_selectors[
    1].producer_subtask = 'bye'
dos.parameters['key'].value_from_oneof.parameter_selectors[
    1].output_parameter_key = '2'

import yaml

with open('file.yaml', 'w') as f:
    yaml.dump(json_format.MessageToDict(dos), f)
