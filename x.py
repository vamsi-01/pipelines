from google.cloud import logging


def stream_cloud_run_logs(
    project: str,
    location: str,
    job_id: str,
):
    client = logging.Client()
    resource_names = [f"projects/{project}"]
    # filter =

    for entry in client.list_log_entries(
            resource_names=resource_names,
            filter_=filter,
    ):
        print(entry.payload)


from google.cloud import logging_v2


def stream_cloud_run_logs(
    project: str,
    location: str,
    job_id: str,
):
    client = logging_v2.services.logging_service_v2.LoggingServiceV2Client()
    request = logging_v2.types.ListLogEntriesRequest(
        resource_names=['projects/cjmccarthy-kfp'],
        filter=f'resource.type="cloud_run_job" AND resource.labels.job_name="{job_id}" AND resource.labels.location="{location}"'
    )
    logs = client.list_log_entries(request=request)
    for log in logs:
        print(log.payload)
    # resource_names = [f"projects/{project}"]
    # filter = f'resource.type="cloud_run_job" AND resource.labels.job_name="{job_id}" AND resource.labels.location="{location}"'

    # while True:
    #     # Fetch log entries
    #     entries = client.list_entries(
    #         resource_names=resource_names, filter_=filter)

    #     for entry in entries:
    #         print(entry.payload)

    #     # Wait for a short period before fetching new logs
    #     sleep(5)


stream_cloud_run_logs(
    project='cjmccarthy-kfp', location='us-central1', job_id='job-2b8p')
