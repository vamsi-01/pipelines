from kfp.components import pipeline_context


class Task:

    def __init__(self) -> None:
        x = pipeline_context.Pipeline.get_default_pipeline()
