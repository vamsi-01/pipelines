import kfp.dsl as dsl


@dsl.pipeline(name='volume-pipeline')
def pipeline():
    # create a PVC from a pre-existing volume
    create_pvc_from_existing_vol = dsl.VolumeOp(
        name='pre_existing_volume',
        generate_unique_name=True,
        resource_name='pvc1',
        size='5Gi',
        storage_class='',
        volume_name='nfs-pv',
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
        pvolumes={'/data': create_pvc_from_existing_vol.volume},
    )

    # # create PVC and dynamically provision volume
    # create_pvc_task = dsl.VolumeOp(
    #     name='create_volume',
    #     generate_unique_name=True,
    #     resource_name='pvc2',
    #     size='1Gi',
    #     storage_class='standard',
    #     modes=dsl.VOLUME_MODE_RWO,
    # )
    # use the previous task's PVC and the newly created PVC
    # task_b = dsl.ContainerOp(
    #     name='step2_gunzip',
    #     image='library/bash:4.4.23',
    #     command=['sh', '-c'],
    #     arguments=['cat /data/step1/file.txt'],
    #     pvolumes={
    #         # '/data': task_a.pvolume,
    #         '/other_data': create_pvc_from_existing_vol.volume
    #     },
    # )


if __name__ == '__main__':
    import warnings

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=ir_file,
    )
