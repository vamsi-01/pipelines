import subprocess
if __name__ == '__main__':
    import os
    import datetime
    import warnings
    import webbrowser

    from kfp import Client

    warnings.filterwarnings('ignore')
    directory = os.path.join(os.path.dirname(__file__), 'examples')
    filename = 'pvc.yaml'
    ir_file = os.path.join(directory, filename)
    subprocess.run(['python', ir_file.replace('yaml', 'py')])
    display_name = datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
    job_id = f"{filename.replace('.yaml', '')}-{display_name}"
    endpoint = 'https://75167a6cffcb723c-dot-us-central1.pipelines.googleusercontent.com'
    kfp_client = Client(host=endpoint)
    run = kfp_client.create_run_from_pipeline_package(ir_file, arguments={})
    url = f'{endpoint}/#/runs/details/{run.run_id}'
    print(url)
    webbrowser.open_new_tab(url)
