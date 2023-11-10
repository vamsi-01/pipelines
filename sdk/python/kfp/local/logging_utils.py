# Copyright 2023 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utilitites for formatting, coloring, and controlling the output of logs."""
import builtins
import contextlib
import datetime
import logging

from kfp import dsl


class Colors:
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    RED = '\033[91m'
    RESET = '\033[0m'


class MillisecondFormatter(logging.Formatter):

    def formatTime(
        self,
        record: logging.LogRecord,
        datefmt: str = None,
    ) -> str:
        created = datetime.datetime.fromtimestamp(record.created)
        s = created.strftime(datefmt)
        # truncate microseconds to milliseconds
        return s[:-3]


@contextlib.contextmanager
def local_logger_context() -> None:
    """Context manager for creating and reseting the local execution logger."""

    logger = logging.getLogger()
    original_level = logger.level
    original_handlers = logger.handlers[:]
    formatter = MillisecondFormatter(
        fmt='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S.%f',
    )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    try:
        yield
    finally:
        logger.setLevel(original_level)
        logger.handlers.clear()
        for handler in original_handlers:
            logger.addHandler(handler)


@contextlib.contextmanager
def indented_print(num_spaces: int = 4) -> None:
    """Context manager to indent all print statements in it's scope by
    num_prints.

    Useful for visually separating a subprocess from the outer process
    logs.
    """
    original_print = builtins.print

    def indented_print_function(*args, **kwargs):
        original_print(' ' * num_spaces, end='')
        return original_print(*args, **kwargs)

    builtins.print = indented_print_function
    try:
        yield
    finally:
        builtins.print = original_print


def color_text(text: str, color: Colors) -> str:
    return f'{color}{text}{Colors.RESET}'


def render_artifact(artifact: dsl.Artifact) -> str:
    fields = ['name', 'uri', 'metadata']
    field_values = ', '.join(
        f'{field}={repr(getattr(artifact, field))}' for field in fields)
    return f'{artifact.__class__.__name__}({field_values})'
