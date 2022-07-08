# In yaml, one can optionally add the type information to both inputs and outputs.
# There are two ways to define the types: string or a dictionary with the openapi_schema_validator property.
# The openapi_schema_validator is a json schema object that describes schema of the parameter value.
component_a = '''\
name: component a
description: component a desc
inputs:
  - {name: field_l, type: Integer}
outputs:
  - {name: field_m, type: {GCSPath: {openapi_schema_validator: {type: string, pattern: "^gs://.*$" } }}}
  - {name: field_n, type: customized_type}
  - {name: field_o, type: GcsUri}
implementation:
  container:
    image: gcr.io/ml-pipeline/component-a
    command: [python3, /pipelines/component/src/train.py]
    args: [
      --field-l, {inputValue: field_l},
    ]
    fileOutputs:
      field_m: /schema.txt
      field_n: /feature.txt
      field_o: /output.txt
'''
component_b = '''\
name: component b
description: component b desc
inputs:
  - {name: field_x, type: customized_type}
  - {name: field_y, type: GcsUri}
  - {name: field_z, type: {GCSPath: {openapi_schema_validator: {type: string, pattern: "^gs://.*$" } }}}
outputs:
  - {name: output_model_uri, type: GcsUri}
implementation:
  container:
    image: gcr.io/ml-pipeline/component-a
    command: [python3]
    args: [
      --field-x, {inputValue: field_x},
      --field-y, {inputValue: field_y},
      --field-z, {inputValue: field_z},
    ]
    fileOutputs:
      output_model_uri: /schema.txt
'''

from kfp.deprecated import compiler
from kfp.deprecated import components as comp
from kfp.deprecated import dsl

# The components are loaded as task factories that generate container_ops.
task_factory_a = comp.load_component_from_text(text=component_a)
task_factory_b = comp.load_component_from_text(text=component_b)


#Use the component as part of the pipeline
@dsl.pipeline(name='type-check-a', description='')
def pipeline_a():
    a = task_factory_a(field_l=12)
    b = task_factory_b(
        field_x=a.outputs['field_n'],
        field_y=a.outputs['field_o'],
        field_z=a.outputs['field_m'])


compiler.Compiler().compile(pipeline_a, 'pipeline_a.zip', type_check=True)

# In this case, the component_a contains an output field_o as GcrUri
# but the component_b requires an input field_y as GcsUri
component_a = '''\
name: component a
description: component a desc
inputs:
  - {name: field_l, type: Integer}
outputs:
  - {name: field_m, type: {GCSPath: {openapi_schema_validator: {type: string, pattern: "^gs://.*$" } }}}
  - {name: field_n, type: customized_type}
  - {name: field_o, type: GcrUri}
implementation:
  container:
    image: gcr.io/ml-pipeline/component-a
    command: [python3, /pipelines/component/src/train.py]
    args: [
      --field-l, {inputValue: field_l},
    ]
    fileOutputs:
      field_m: /schema.txt
      field_n: /feature.txt
      field_o: /output.txt
'''
component_b = '''\
name: component b
description: component b desc
inputs:
  - {name: field_x, type: customized_type}
  - {name: field_y, type: GcsUri}
  - {name: field_z, type: {GCSPath: {openapi_schema_validator: {type: string, pattern: "^gs://.*$" } }}}
outputs:
  - {name: output_model_uri, type: GcsUri}
implementation:
  container:
    image: gcr.io/ml-pipeline/component-a
    command: [python3]
    args: [
      --field-x, {inputValue: field_x},
      --field-y, {inputValue: field_y},
      --field-z, {inputValue: field_z},
    ]
    fileOutputs:
      output_model_uri: /schema.txt
'''

from kfp.deprecated import compiler
from kfp.deprecated import components
from kfp.deprecated import dsl
from kfp.deprecated.dsl.types import InconsistentTypeException

task_factory_a = components.load_component_from_text(text=component_a)
task_factory_b = components.load_component_from_text(text=component_b)


#Use the component as part of the pipeline
@dsl.pipeline(name='type-check-b', description='')
def pipeline_b():
    a = task_factory_a(field_l=12)
    b = task_factory_b(
        field_x=a.outputs['field_n'],
        field_y=a.outputs['field_o'],
        field_z=a.outputs['field_m'])


try:
    compiler.Compiler().compile(pipeline_b, 'pipeline_b.zip', type_check=True)
except InconsistentTypeException as e:
    print(e)
