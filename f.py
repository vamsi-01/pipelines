import kfp.dsl as dsl
from kfp import components

comp = components.load_component_from_text("""
name: echo hello


implementation:
  container:
    image: alpine
    command:
      - echo
      - hello""")

comp = dsl.ContainerOp(name='echo hello', container=)
image='library/bash:4.4.23',
        command=['sh', '-c'],
        arguments=['mkdir /data/step2 && '
                   'gunzip /data/step1/file1.gz -c >/data/step2/file1'],

@dsl.pipeline(name='secret-as-env-var-1')
def pipeline():
    task = comp()
    task.apply(
        onprem.use_k8s_secret(
            secret_name='my-secret',
            k8s_secret_key_to_env={'password': 'SECRET_VAR'}))
