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
            'mkdir /mounted_data/step1 && '
            'echo hello > /mounted_data/step1/file.txt'
        ],
        pvolumes={
            '/mounted_data': dynamically_provisioned_vol.volume
        },
    ).add_pod_annotation(name="pipelines.kubeflow.org/max_cache_staleness",
                         value="P0D")

    gunzip = dsl.ContainerOp(
        name='unzip',
        image='library/bash:4.4.23',
        command=['sh', '-c'],
        arguments=['cat /data/step1/file.txt'],
        pvolumes={
            # '/data': task_a.pvolume,
            '/data': ingest.pvolume
        },
    )
    dynamically_provisioned_vol.delete().after(gunzip)


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
