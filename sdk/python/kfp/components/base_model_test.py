# Copyright 2022 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import dataclasses
import functools
from typing import (Any, Dict, List, Mapping, MutableMapping, MutableSequence,
                    Optional, OrderedDict, Sequence, Set, Tuple, Union)
import unittest

from absl.testing import parameterized
from kfp.components import base_model


class TypeClass(base_model.BaseModel):
    a: str
    b: List[int]
    c: Dict[str, int]
    d: Union[int, str]
    e: Union[int, str, bool]
    f: Optional[int]


class TestBaseModel(unittest.TestCase):

    def test_is_dataclass(self):

        class Child(base_model.BaseModel):
            x: int

        child = Child(x=1)
        self.assertTrue(dataclasses.is_dataclass(child))

    def test_can_create_properties_using_attributes(self):

        class Child(base_model.BaseModel):
            x: Optional[int]

            @property
            def prop(self) -> bool:
                return self.x is not None

        child1 = Child(x=None)
        self.assertEqual(child1.prop, False)

        child2 = Child(x=1)
        self.assertEqual(child2.prop, True)

    def test_unsupported_type_success(self):

        class OtherClass(base_model.BaseModel):
            x: int

        class MyClass(base_model.BaseModel):
            a: OtherClass

    def test_unsupported_type_failures(self):

        with self.assertRaisesRegex(TypeError, r'not a supported'):

            class MyClass(base_model.BaseModel):
                a: tuple

        with self.assertRaisesRegex(TypeError, r'not a supported'):

            class MyClass(base_model.BaseModel):
                a: Tuple

        with self.assertRaisesRegex(TypeError, r'not a supported'):

            class MyClass(base_model.BaseModel):
                a: Set

        with self.assertRaisesRegex(TypeError, r'not a supported'):

            class OtherClass:
                pass

            class MyClass(base_model.BaseModel):
                a: OtherClass

    def test_base_model_validation(self):

        # test exception thrown
        class MyClass(base_model.BaseModel):
            x: int

            def _validate_x(self) -> None:
                if self.x < 2:
                    raise ValueError('x must be greater than 2')

        with self.assertRaisesRegex(ValueError, 'x must be greater than 2'):
            mc = MyClass(x=1)

        # test value modified same type
        class MyClass(base_model.BaseModel):
            x: int

            def _validate_x(self) -> None:
                self.x = max(self.x, 2)

        mc = MyClass(x=1)
        self.assertEqual(mc.x, 2)

        # test value modified new type
        class MyClass(base_model.BaseModel):
            x: Optional[List[int]] = None

            def _validate_x(self) -> None:
                if isinstance(self.x, list) and not self.x:
                    self.x = None

        mc = MyClass(x=[])
        self.assertEqual(mc.x, None)

    def test_can_set_field(self):

        class MyClass(base_model.BaseModel):
            x: int

        mc = MyClass(x=2)
        mc.x = 1
        self.assertEqual(mc.x, 1)

    def test_can_use_default_factory(self):

        class MyClass(base_model.BaseModel):
            x: List[int] = dataclasses.field(default_factory=list)

        mc = MyClass()
        self.assertEqual(mc.x, [])


class TestIsBaseModel(unittest.TestCase):

    def test_true(self):
        self.assertEqual(base_model._is_basemodel(base_model.BaseModel), True)

        class MyClass(base_model.BaseModel):
            pass

        self.assertEqual(base_model._is_basemodel(MyClass), True)

    def test_false(self):
        self.assertEqual(base_model._is_basemodel(int), False)
        self.assertEqual(base_model._is_basemodel(1), False)
        self.assertEqual(base_model._is_basemodel(str), False)


class TestLoadBaseModelHelper(parameterized.TestCase):

    def setUp(self):
        self.no_alias_load_base_model_helper = functools.partial(
            base_model._load_basemodel_helper, by_alias=False)

    def test_load_primitive(self):
        self.assertEqual(self.no_alias_load_base_model_helper(str, 'a'), 'a')
        self.assertEqual(self.no_alias_load_base_model_helper(int, 1), 1)
        self.assertEqual(self.no_alias_load_base_model_helper(float, 1.0), 1.0)
        self.assertEqual(self.no_alias_load_base_model_helper(bool, True), True)
        self.assertEqual(
            self.no_alias_load_base_model_helper(type(None), None), None)

    def test_load_primitive_with_casting(self):
        self.assertEqual(self.no_alias_load_base_model_helper(int, '1'), 1)
        self.assertEqual(self.no_alias_load_base_model_helper(str, 1), '1')
        self.assertEqual(self.no_alias_load_base_model_helper(float, 1), 1.0)
        self.assertEqual(self.no_alias_load_base_model_helper(int, 1.0), 1)
        self.assertEqual(self.no_alias_load_base_model_helper(bool, 1), True)
        self.assertEqual(self.no_alias_load_base_model_helper(bool, 0), False)
        self.assertEqual(self.no_alias_load_base_model_helper(int, True), 1)
        self.assertEqual(self.no_alias_load_base_model_helper(int, False), 0)
        self.assertEqual(
            self.no_alias_load_base_model_helper(bool, None), False)

    def test_load_none(self):
        self.assertEqual(
            self.no_alias_load_base_model_helper(type(None), None), None)
        with self.assertRaisesRegex(TypeError, ''):
            self.no_alias_load_base_model_helper(type(None), 1)

    @parameterized.parameters(['a', 1, 1.0, True, False, None, ['list']])
    def test_load_any(self, data: Any):
        self.assertEqual(self.no_alias_load_base_model_helper(Any, data),
                         data)  # type: ignore

    def test_load_list(self):
        self.assertEqual(
            self.no_alias_load_base_model_helper(List[str], ['a']), ['a'])
        self.assertEqual(
            self.no_alias_load_base_model_helper(List[int], [1, 1]), [1, 1])
        self.assertEqual(
            self.no_alias_load_base_model_helper(List[float], [1.0]), [1.0])
        self.assertEqual(
            self.no_alias_load_base_model_helper(List[bool], [True]), [True])
        self.assertEqual(
            self.no_alias_load_base_model_helper(List[type(None)], [None]),
            [None])

    def test_load_primitive_other_iterables(self):
        self.assertEqual(
            self.no_alias_load_base_model_helper(Sequence[bool], [True]),
            [True])
        self.assertEqual(
            self.no_alias_load_base_model_helper(MutableSequence[type(None)],
                                                 [None]), [None])
        self.assertEqual(
            self.no_alias_load_base_model_helper(Sequence[str], ['a']), ['a'])

    def test_load_dict(self):
        self.assertEqual(
            self.no_alias_load_base_model_helper(Dict[str, str], {'a': 'a'}),
            {'a': 'a'})
        self.assertEqual(
            self.no_alias_load_base_model_helper(Dict[str, int], {'a': 1}),
            {'a': 1})
        self.assertEqual(
            self.no_alias_load_base_model_helper(Dict[str, float], {'a': 1.0}),
            {'a': 1.0})
        self.assertEqual(
            self.no_alias_load_base_model_helper(Dict[str, bool], {'a': True}),
            {'a': True})
        self.assertEqual(
            self.no_alias_load_base_model_helper(Dict[str, type(None)],
                                                 {'a': None}), {'a': None})

    def test_load_mapping(self):
        self.assertEqual(
            self.no_alias_load_base_model_helper(Mapping[str, float],
                                                 {'a': 1.0}), {'a': 1.0})
        self.assertEqual(
            self.no_alias_load_base_model_helper(MutableMapping[str, bool],
                                                 {'a': True}), {'a': True})
        self.assertEqual(
            self.no_alias_load_base_model_helper(OrderedDict[str,
                                                             type(None)],
                                                 {'a': None}), {'a': None})

    def test_load_union_types(self):
        self.assertEqual(
            self.no_alias_load_base_model_helper(Union[str, int], 'a'), 'a')
        self.assertEqual(
            self.no_alias_load_base_model_helper(Union[str, int], 1), '1')
        self.assertEqual(
            self.no_alias_load_base_model_helper(Union[int, str], 1), 1)
        self.assertEqual(
            self.no_alias_load_base_model_helper(Union[int, str], '1'), 1)

    def test_load_optional_types(self):
        self.assertEqual(
            self.no_alias_load_base_model_helper(Optional[str], 'a'), 'a')
        self.assertEqual(
            self.no_alias_load_base_model_helper(Optional[str], None), None)

    def test_unsupported_type(self):
        with self.assertRaisesRegex(TypeError, r'Unsupported type:'):
            self.no_alias_load_base_model_helper(Set[int], {1})


class TestGetOriginPy37(parameterized.TestCase):

    def test_is_same_as_typing_version(self):
        import sys
        if sys.version_info.major == 3 and sys.version_info.minor >= 8:
            import typing
            for field in dataclasses.fields(TypeClass):
                self.assertEqual(
                    base_model._get_origin_py37(field.type),
                    typing.get_origin(field.type))


class TestGetArgsPy37(parameterized.TestCase):

    def test_is_same_as_typing_version(self):
        import sys
        if sys.version_info.major == 3 and sys.version_info.minor >= 8:
            import typing
            for field in dataclasses.fields(TypeClass):
                self.assertEqual(
                    base_model._get_args_py37(field.type),
                    typing.get_args(field.type))


if __name__ == '__main__':
    unittest.main()
