import os
from typing import Dict, Generic, List, TypeVar, Union

from kfp import dsl
from kfp.dsl import Dataset
from kfp.dsl import Model

A = TypeVar('A')
Key = Union[str, int]


class InputMap(Generic[A]):
    # can also support iteration, if desired

    def __init__(self, input_artifacts: Dict[Key, A]) -> None:
        # populate the store with inputs
        self._store = input_artifacts

    def keys(self) -> List[Key]:
        """Gets a list of all keys."""
        return list(self._store.keys())

    # indexing
    def get(self, k: Key) -> A:
        """Gets the input artifact for key k."""
        return self._store[k]

    # can also support iteration over input artifacts
    def __iter__(self):
        ...

    def __next__(self):
        ...


class OutputMap(Generic[A]):
    """Name...

    OutputGeneratorMap?
    """

    def __init__(self, root_artifact: A) -> None:
        # root artifact needed to generate new artifacts
        self._root_artifact = root_artifact
        # populate the store with an empty dict
        self._store: Dict[Key, A] = {}

    def keys(self) -> List[Key]:
        """Gets a list of all keys."""
        return list(self._store.keys())

    # indexing
    def get(self, k: Key) -> A:
        """Gets the input artifact for key k."""
        self._store[k] = self._make_artifact(k)
        return self._store[k]

    def _validate_key(self, k: Key) -> None:
        """Checks that k can be used as a directory name."""
        ...

    def _make_artifact(self, k: Key) -> A:
        """Makes and returns an artifact using the user-provided key and the
        root artifact."""
        self._validate_key(k)
        return self._root_artifact.__class__(
            name='',
            uri=os.path.join(self._root_artifact.uri, str(k)),
            metadata={})

    # can also support iteration over artifacts that have already been created
    # note: I prefer not to support this -- it introduces a larger usage gap between InputMap and OutputMap
    def __iter__(self):
        ...

    def __next__(self):
        ...


@dsl.component
def train(datasets: InputMap[Dataset], models: OutputMap[Model]):
    """Train a model on multiple different datasets."""
    for key in datasets.keys():
        dataset = datasets.get(key)
        model = models.get(key)
        train_model(dataset, model)


@dsl.component
def parallelfor_collector(datasets: InputMap[Dataset]):
    """Uses output from an upstream ParallelFor."""
    for key in datasets.keys():
        dataset = datasets.get(key)
