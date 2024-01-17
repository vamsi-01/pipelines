class Entrypoint:

    def __init__(self, func):
        self.func = func

    def __call__(self, *args, **kwargs):
        self.func(*args, **kwargs)


def entrypoint(func):
    return Entrypoint(func=func)
