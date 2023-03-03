from kfp import dsl
from kfp import kubernetes


@dsl.component
def comp():
    pass


@dsl.pipeline
def my_pipeline():
    # dynamically provision a PV and use it in two tasks
    pvc1 = kubernetes.CreatePVC(
        pvc_name_suffix='-my-pvc',
        access_modes=['ReadWriteMany'],
        size='5Gi',
        storage_class='standard',
    )
    task1 = comp()
    kubernetes.mount_pvc(
        task1,
        pvc_name=pvc1.outputs['name'],
        mount_path='/data',
    )

    # reuse
    task2 = comp().after(task1)
    kubernetes.mount_pvc(
        task2, pvc_name=pvc1.outputs['name'], mount_path='/reused_data')

    delete_pvc1 = kubernetes.DeletePVC(
        pvc_name=pvc1.outputs['name']).after(task2)

    # use an existing PVC
    task3 = comp()
    kubernetes.mount_pvc(
        task3,
        pvc_name='my-existing-pvc',
        mount_path='/data',
    )


if __name__ == '__main__':
    import datetime
    import warnings
    import webbrowser

    from kfp import compiler

    from kfp import client

    warnings.filterwarnings('ignore')
    ir_file = __file__.replace('.py', '.yaml')
    compiler.Compiler().compile(pipeline_func=my_pipeline, package_path=ir_file)
    pipeline_name = __file__.split('/')[-1].replace('_', '-').replace('.py', '')
    display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
    job_id = f'{pipeline_name}-{display_name}'
    endpoint = 'https://75167a6cffcb723c-dot-us-central1.pipelines.googleusercontent.com'
    kfp_client = client.Client(host=endpoint)
    run = kfp_client.create_run_from_pipeline_package(ir_file)
    url = f'{endpoint}/#/runs/details/{run.run_id}'
    print(url)
    webbrowser.open_new_tab(url)