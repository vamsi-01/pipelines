import os
from typing import NamedTuple

from google_cloud_pipeline_components.types import artifact_types
from google_cloud_pipeline_components.types.artifact_types import \
    UnmanagedContainerModel
from kfp import compiler
from kfp import components
from kfp import dsl
from kfp.components import importer_node
from kfp.dsl import Input

project_root = os.path.abspath(os.path.join(__file__, *([os.path.pardir] * 6)))
component_path = os.path.join(project_root, 'components', 'google-cloud',
                              'google_cloud_pipeline_components', 'v1', 'model',
                              'upload_model', 'component.yaml')
gcpc_component = components.load_component_from_file(component_path)

KFP_PACKAGE_PATH = 'git+https://github.com/connor-mccarthy/pipelines@support-custom-artifact-types#subdirectory=sdk/python'
GCPC_PACKAGE_PATH = 'git+https://github.com/connor-mccarthy/pipelines@support-custom-artifact-types#subdirectory=components/google-cloud'


@dsl.component(
    kfp_package_path=KFP_PACKAGE_PATH,
    packages_to_install=[GCPC_PACKAGE_PATH],
)
def dummy_op(
    artifact: Input[artifact_types.UnmanagedContainerModel]
) -> artifact_types.UnmanagedContainerModel:
    print('artifact.type: ', type(artifact))
    print('artifact.name: ', artifact.name)
    print('artifact.uri: ', artifact.uri)
    print('artifact.metadata: ', artifact.metadata)
    return artifact


@dsl.component(
    kfp_package_path=KFP_PACKAGE_PATH,
    packages_to_install=[GCPC_PACKAGE_PATH],
)
def named_tuple_op(
    artifact: Input[UnmanagedContainerModel]
) -> NamedTuple('NamedTupleOutput',
                [('model1', artifact_types.UnmanagedContainerModel),
                 ('model2', artifact_types.UnmanagedContainerModel)]):
    NamedTupleOutput = NamedTuple(
        'NamedTupleOutput',
        [('model1', artifact_types.UnmanagedContainerModel),
         ('model2', artifact_types.UnmanagedContainerModel)])
    return NamedTupleOutput(model1=artifact, model2=artifact)


@dsl.pipeline(name='pipeline-with-gcpc-types')
def my_pipeline():
    importer_spec = importer_node.importer(
        artifact_uri='gs://managed-pipeline-gcpc-e2e-test/automl-tabular/model',
        artifact_class=artifact_types.UnmanagedContainerModel,
        metadata={
            'containerSpec': {
                'imageUri':
                    'us-docker.pkg.dev/vertex-ai/automl-tabular/prediction-server:prod'
            }
        })
    dummy_task = dummy_op(artifact=importer_spec.outputs['artifact'])

    named_tuple_op(artifact=dummy_task.output)


if __name__ == '__main__':
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
