from kfp import components
from kfp import onprem
import kfp.dsl as dsl

comp = components.load_component_from_text("""
name: echo hello


implementation:
  container:
    image: alpine
    command:
      - echo
      - hello""")


@dsl.pipeline(name='secret-as-env-var-1')
def pipeline():
    task = dsl.ContainerOp(name='echo hello',
                           image='alpine',
                           command=['echo', 'hello'])
    task.apply(
        onprem.use_k8s_secret(secret_name='my-secret',
                              k8s_secret_key_to_env={'password':
                                                     'SECRET_VAR'}))


if __name__ == '__main__':
    import warnings

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=ir_file,
    )
