from kfp.dsl import pipeline_channel

pchan = pipeline_channel.PipelineParameterChannel(
    name='foo', channel_type='STRING', task_name='')
