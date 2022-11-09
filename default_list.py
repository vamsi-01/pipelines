from collections import defaultdict
import os
from typing import Generic, TypeVar, Union

from kfp import dsl
from kfp.dsl import Artifact
from kfp.dsl import Dataset
from kfp.dsl import Model

A = TypeVar('A')
Key = Union[str, int]


class InputArtifactDict(Generic[A]):
    ...


class OutputArtifactDict(Generic[A]):
    ...


class ArtifactFactory:

    def __init__(self, root_artifact: A) -> None:
        self._root_artifact = root_artifact

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

    def __call__(self, k: str):
        return self._make_artifact(k)


class keygenerated_defaultdict(defaultdict):

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)

        ret = self[key] = self.default_factory(key)
        return ret


root_artifact = Artifact(name='', uri='gs://bucket/rootdir', metadata={})
artifact_dict = keygenerated_defaultdict(ArtifactFactory(root_artifact))

# demo
## access two artifacts to create them
artifact_dict['custom_key']
artifact_dict['another_key']
assert len(artifact_dict) == 2


@dsl.component
def train(datasets: InputArtifactDict[Dataset],
          models: OutputArtifactDict[Model]):
    """Train a model on multiple different datasets."""
    # all datasets are already created
    for key, dataset in datasets.items():
        # model is created here
        model = models[key]
        train_model(dataset, model)


@dsl.component
def parallelfor_collector(datasets: InputArtifactDict[Dataset]):
    """Uses output from an upstream ParallelFor."""
    for key, dataset in datasets.items():
        print(key)
        print(dataset)
