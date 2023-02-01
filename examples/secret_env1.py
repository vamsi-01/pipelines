from kfp import onprem
import kfp.dsl as dsl


@dsl.pipeline(name='secret-as-env-var-1')
def pipeline():
    task = dsl.ContainerOp(name='echo-secret',
                           image='alpine',
                           command=['sh', '-c', 'echo $SECRET_VAR'])
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
