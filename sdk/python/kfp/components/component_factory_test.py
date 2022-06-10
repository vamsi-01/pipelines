# Copyright 2022 The Kubeflow Authors
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

import inspect
import os
import sys
import tempfile
import textwrap
from typing import Any, Dict
import unittest

from absl.testing import parameterized
from kfp import dsl
from kfp.components import component_factory
from kfp.components.types import artifact_types
from kfp.components.types.artifact_types import Artifact
from kfp.components.types.artifact_types import Dataset
from kfp.components.types.type_annotations import Input
from kfp.components.types.type_annotations import InputPath
from kfp.components.types.type_annotations import Output
from kfp.components.types.type_annotations import OutputPath


class TestGetPackagesToInstallCommand(unittest.TestCase):

    def test_with_no_packages_to_install(self):
        packages_to_install = []

        command = component_factory._get_packages_to_install_command(
            packages_to_install)
        self.assertEqual(command, [])

    def test_with_packages_to_install_and_no_pip_index_url(self):
        packages_to_install = ['package1', 'package2']

        command = component_factory._get_packages_to_install_command(
            packages_to_install)
        concat_command = ' '.join(command)
        for package in packages_to_install:
            self.assertTrue(package in concat_command)

    def test_with_packages_to_install_with_pip_index_url(self):
        packages_to_install = ['package1', 'package2']
        pip_index_urls = ['https://myurl.org/simple']

        command = component_factory._get_packages_to_install_command(
            packages_to_install, pip_index_urls)
        concat_command = ' '.join(command)
        for package in packages_to_install + pip_index_urls:
            self.assertTrue(package in concat_command)


Alias = Artifact
artifact_types_alias = artifact_types


class TestGetSignatureFromFunc(unittest.TestCase):

    def test_no_annotations(self):

        def func(a, b):
            pass

        actual = component_factory.get_signature_from_func(func)
        expected = 'def func(a, b):'
        self.assertEqual(actual, expected)

    def test_no_return(self):

        def func(a: int, b: Input[Artifact]):
            pass

        actual = component_factory.get_signature_from_func(func)
        expected = 'def func(a: int, b: Input[Artifact]):'
        self.assertEqual(actual, expected)

    def test_with_return(self):

        def func(a: int, b: Input[Artifact]) -> int:
            return 1

        actual = component_factory.get_signature_from_func(func)
        expected = 'def func(a: int, b: Input[Artifact]) -> int:'
        self.assertEqual(actual, expected)

    def test_multiline(self):

        def func(
            a: int,
            b: Input[Artifact],
        ) -> int:
            return 1

        actual = component_factory.get_signature_from_func(func)
        expected = 'def func(\n    a: int,\n    b: Input[Artifact],\n) -> int:'
        self.assertEqual(actual, expected)

    def test_alias(self):

        def func(a: int, b: Input[Alias]):
            pass

        actual = component_factory.get_signature_from_func(func)
        expected = 'def func(a: int, b: Input[Alias]):'
        self.assertEqual(actual, expected)

    def test_long_form_annotation(self):

        def func(a: int, b: Input[artifact_types.Artifact]):
            pass

        actual = component_factory.get_signature_from_func(func)
        expected = 'def func(a: int, b: Input[artifact_types.Artifact]):'
        self.assertEqual(actual, expected)

    def test_input_and_output_paths(self):

        def func(a: int, b: InputPath('Dataset'), c: OutputPath('Dataset')):
            pass

        actual = component_factory.get_signature_from_func(func)
        expected = "def func(a: int, b: InputPath('Dataset'), c: OutputPath('Dataset')):"
        self.assertEqual(actual, expected)


class TestGetParamToAnnStringFromSignature(parameterized.TestCase):

    @parameterized.parameters([
        ('def func(a, b):', {
            'a': None,
            'b': None
        }),
        ('def func(a: int, b: Input[Artifact]):', {
            'a': 'int',
            'b': 'Input[Artifact]'
        }),
        ('def func(a: int, b: Input[Artifact]) -> int:', {
            'a': 'int',
            'b': 'Input[Artifact]',
            'return': 'int'
        }),
        ('def func(\n    a: int,\n    b: Input[Artifact],\n) -> int:', {
            'a': 'int',
            'b': 'Input[Artifact]',
            'return': 'int'
        }),
        ('def func(a: int, b: Input[Alias]):', {
            'a': 'int',
            'b': 'Input[Alias]'
        }),
        ('def func(a: int, b: Input[artifact_types.Artifact]):', {
            'a': 'int',
            'b': 'Input[artifact_types.Artifact]'
        }),
    ])
    def test(self, signature: str, expected: Dict[str, str]):
        self.assertEqual(
            component_factory.get_param_to_ann_string_from_signature(signature),
            expected)


class TestGetParamToAnnString(unittest.TestCase):

    def test_no_annotations(self):

        def func(a, b):
            pass

        actual = component_factory.get_param_to_ann_string(func)
        expected = {'a': None, 'b': None}
        self.assertEqual(actual, expected)

    def test_no_return(self):

        def func(a: int, b: Input[Artifact]):
            pass

        actual = component_factory.get_param_to_ann_string(func)
        expected = {'a': 'int', 'b': 'Input[Artifact]'}
        self.assertEqual(actual, expected)

    def test_with_return(self):

        def func(a: int, b: Input[Artifact]) -> int:
            return 1

        actual = component_factory.get_param_to_ann_string(func)
        expected = {'a': 'int', 'b': 'Input[Artifact]', 'return': 'int'}
        self.assertEqual(actual, expected)

    def test_multiline(self):

        def func(
            a: int,
            b: Input[Artifact],
        ) -> int:
            return 1

        actual = component_factory.get_param_to_ann_string(func)
        expected = {'a': 'int', 'b': 'Input[Artifact]', 'return': 'int'}
        self.assertEqual(actual, expected)

    def test_alias(self):

        def func(a: int, b: Input[Alias]):
            pass

        actual = component_factory.get_param_to_ann_string(func)
        expected = {'a': 'int', 'b': 'Input[Alias]'}
        self.assertEqual(actual, expected)

    def test_long_form_annotation(self):

        def func(a: int, b: Input[artifact_types.Artifact]):
            pass

        actual = component_factory.get_param_to_ann_string(func)
        expected = {'a': 'int', 'b': 'Input[artifact_types.Artifact]'}
        self.assertEqual(actual, expected)


class MyCustomArtifact:
    TYPE_NAME = 'my_custom_artifact'


class _TestCaseWithThirdPartyPackage(parameterized.TestCase):

    @classmethod
    def setUpClass(cls):

        class ThirdPartyArtifact(artifact_types.Artifact):
            TYPE_NAME = 'custom.my_third_party_artifact'

        class_source = 'from kfp.components.types import artifact_types\n\n' + textwrap.dedent(
            inspect.getsource(ThirdPartyArtifact))

        tmp_dir = tempfile.TemporaryDirectory()
        with open(os.path.join(tmp_dir.name, 'my_package.py'), 'w') as f:
            f.write(class_source)
        sys.path.append(tmp_dir.name)
        cls.tmp_dir = tmp_dir

    @classmethod
    def teardownClass(cls):
        sys.path.pop()
        cls.tmp_dir.cleanup()


class TestGetFullQualnameForClass(_TestCaseWithThirdPartyPackage):

    @parameterized.parameters([
        (Alias, 'kfp.components.types.artifact_types.Artifact'),
        (Artifact, 'kfp.components.types.artifact_types.Artifact'),
        (Dataset, 'kfp.components.types.artifact_types.Dataset'),
    ])
    def test(self, obj: Any, expected_qualname: str):
        self.assertEqual(
            component_factory.get_full_qualname_for_annotation(obj),
            expected_qualname)

    def test_my_package_artifact(self):
        import my_package
        self.assertEqual(
            component_factory.get_full_qualname_for_annotation(
                my_package.ThirdPartyArtifact), 'my_package.ThirdPartyArtifact')


class GetArtifactImportItemsFromFunction(_TestCaseWithThirdPartyPackage):

    def test_no_annotations(self):

        def func(a, b):
            pass

        actual = component_factory.get_artifact_import_items_from_function(func)
        expected = []
        self.assertEqual(actual, expected)

    def test_no_return(self):
        from my_package import ThirdPartyArtifact

        def func(a: int, b: Input[ThirdPartyArtifact]):
            pass

        actual = component_factory.get_artifact_import_items_from_function(func)
        expected = [{
            'name': 'my_package.ThirdPartyArtifact',
            'alias': 'ThirdPartyArtifact'
        }]
        self.assertEqual(actual, expected)

    def test_with_return(self):
        from my_package import ThirdPartyArtifact

        def func(a: int, b: Input[ThirdPartyArtifact]) -> int:
            return 1

        actual = component_factory.get_artifact_import_items_from_function(func)
        expected = [{
            'name': 'my_package.ThirdPartyArtifact',
            'alias': 'ThirdPartyArtifact'
        }]
        self.assertEqual(actual, expected)

    def test_multiline(self):
        from my_package import ThirdPartyArtifact

        def func(
            a: int,
            b: Input[ThirdPartyArtifact],
        ) -> int:
            return 1

        actual = component_factory.get_artifact_import_items_from_function(func)
        expected = [{
            'name': 'my_package.ThirdPartyArtifact',
            'alias': 'ThirdPartyArtifact'
        }]
        self.assertEqual(actual, expected)

    def test_alias(self):
        from my_package import ThirdPartyArtifact
        Alias = ThirdPartyArtifact

        def func(a: int, b: Input[Alias]):
            pass

        actual = component_factory.get_artifact_import_items_from_function(func)
        expected = [{'name': 'my_package.ThirdPartyArtifact', 'alias': 'Alias'}]
        self.assertEqual(actual, expected)

    def test_long_form_annotation(self):
        import my_package

        def func(a: int, b: Output[my_package.ThirdPartyArtifact]):
            pass

        actual = component_factory.get_artifact_import_items_from_function(func)
        expected = [{'name': 'my_package', 'alias': 'my_package'}]
        self.assertEqual(actual, expected)

    def test_aliased_module_throws_error(self):
        import my_package as my_package_alias

        def func(a: int, b: Output[my_package_alias.ThirdPartyArtifact]):
            pass

        with self.assertRaisesRegex(
                TypeError,
                'due to an aliased module used in the type annotation'):
            component_factory.get_artifact_import_items_from_function(func)


class TestImportArtifactAnnotations(_TestCaseWithThirdPartyPackage):

    def test(self):
        import_items = [
            {
                'name': 'kfp.components.types.artifact_types.Dataset',
                'alias': 'my_alias'
            },
            {
                'name': 'typing.TypeVar',
                'alias': 'type_var_alias'
            },
        ]
        aliases = [item['alias'] for item in import_items]

        for alias in aliases:
            self.assertNotIn(alias, globals())

        component_factory.import_artifact_annotations(import_items, globals())

        for alias in aliases:
            self.assertIn(alias, globals())


if __name__ == '__main__':
    unittest.main()
