import kfp.dsl as dsl
from kfp.dsl.extensions import kubernetes


@dsl.pipeline(name='secret-as-volume')
def pipeline():
    task = dsl.ContainerOp(name='echo-secret',
                           image='alpine',
                           command=['sh', '-c', 'cat $SECRET_VAR'])
    task.apply(
        kubernetes.use_secret(
            secret_name='my-secret',
            secret_volume_mount_path='/mnt/my_vol',
            secret_file_path_in_volume='password',
            env_variable='SECRET_VAR',
        ))


if __name__ == '__main__':
    import warnings

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=ir_file,
    )
