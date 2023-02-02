import kfp.dsl as dsl


@dsl.pipeline(name='volume-pipeline')
def pipeline():
    # dynamically create a PVC
    dynamically_provisioned_vol = dsl.VolumeOp(
        name='dynamically_provisioned_vol',
        generate_unique_name=True,
        resource_name='pvc1',
        size='5Gi',
        storage_class='standard',
        # volume_name='nfs-pv',
        modes=dsl.VOLUME_MODE_RWO,
    )

    # ingest data
    ingest = dsl.ContainerOp(
        name='ingest',
        image='alpine',
        command=['sh', '-c'],
        arguments=[
            'mkdir /mounted_data/pipeline && '
            'echo hello > /mounted_data/pipeline/file.txt'
        ],
        pvolumes={'/mounted_data': dynamically_provisioned_vol.volume},
    )

    # cat out data from previous step
    cat = dsl.ContainerOp(
        name='cat',
        image='alpine',
        command=['sh', '-c'],
        arguments=['cat /data/pipeline/file.txt'],
        pvolumes={'/data': ingest.pvolume},
    )

    # clean up PVC (requires pod deletion first)
    dynamically_provisioned_vol.delete().after(cat)


# I expect the VolumeOp to create a PVC from nfs-pv
# I expect 'step1_ingest' to use it

if __name__ == '__main__':
    import warnings

    from kfp import compiler

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(
        pipeline_func=pipeline,
        package_path=ir_file,
    )
