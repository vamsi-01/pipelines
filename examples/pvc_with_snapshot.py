import kfp.dsl as dsl


@dsl.pipeline(name='volume-pipeline')
def pipeline():
    # create PVC and dynamically provision volume
    create_pvc_task = dsl.VolumeOp(
        name='create_volume',
        generate_unique_name=True,
        resource_name='pvc1',
        size='1Gi',
        storage_class='standard',
        modes=dsl.VOLUME_MODE_RWM,
    )

    # use PVC; write to file
    task_a = dsl.ContainerOp(
        name='step1_ingest',
        image='alpine',
        command=['sh', '-c'],
        arguments=[
            'mkdir /data/step1 && '
            'echo hello > /data/step1/file1.txt'
        ],
        pvolumes={'/data': create_pvc_task.volume},
    )

    snapshot_volume_task = dsl.VolumeSnapshotOp(name='step1_snap',
                                                resource_name='step1_snap',
                                                volume=task_a.pvolume)

    create_volume_task = dsl.VolumeOp(
        name='volume_from_snapshot',
        resource_name='vol1',
        size='1Gi',
        modes=dsl.VOLUME_MODE_RWM,
        data_source=snapshot_volume_task.snapshot)


if __name__ == '__main__':
    import warnings

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=ir_file,
    )
