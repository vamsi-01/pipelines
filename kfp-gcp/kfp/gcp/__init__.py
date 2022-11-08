def add_secret(
    pipeline_task: 'PipelineTask',
    secret: str,
    value: str,
) -> 'PipelineTask':
    secrets = pipeline_task.platform_configuration.get('gcp',
                                                       {}).get('secrets', [])
    secrets.append({secret: value})
    if 'gcp' not in pipeline_task.platform_configuration:
        pipeline_task.platform_configuration['gcp'] = {}
    pipeline_task.platform_configuration['gcp']['secrets'] = secrets
    return pipeline_task
