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
import unittest
from collections import abc
from typing import (Any, Dict, List, Mapping, MutableMapping, MutableSequence,
                    Optional, OrderedDict, Sequence, Set, Tuple, Union)

from absl.testing import parameterized
from kfp.components import base_model


class TypeClass(base_model.BaseModel):
    a: str
    b: list[int]
    c: List[int]
    d: Dict[str, int]
    e: Union[int, str]
    f: Union[int, str, bool]
    g: Optional[int]


class TestSnakeCaseToCamelCase(parameterized.TestCase):

    @parameterized.parameters({('', ''), ('a', 'a'), ('a_b', 'aB'),
                               ('a_b_c', 'aBC'),
                               ('long_example_string', 'longExampleString'),
                               ('aB', 'aB'), ('a__b', 'aB')})
    def test_snake_to_snake(self, snake, camel):
        actual = base_model._snake_case_to_camel_case(snake)
        self.assertEqual(actual, camel)


class TestBaseModel(unittest.TestCase):

    def test_is_dataclass(self):

        class Child(base_model.BaseModel):
            x: int

        child = Child(x=1)
        self.assertTrue(dataclasses.is_dataclass(child))

    def test_to_dict_simple(self):

        class Child(base_model.BaseModel):
            i: int
            s: str
            f: float
            l: List[int]

        data = {'i': 1, 's': 's', 'f': 1.0, 'l': ['a', 'b']}
        child = Child(**data)
        actual = child.to_dict()
        self.assertEqual(actual, data)

    def test_to_dict_nested(self):

        class InnerChild(base_model.BaseModel):
            a: str

        class Child(base_model.BaseModel):
            b: int
            c: InnerChild

        data = {'b': 1, 'c': InnerChild(a='a')}
        child = Child(**data)
        actual = child.to_dict()
        expected = {'b': 1, 'c': {'a': 'a'}}
        self.assertEqual(actual, expected)

    def test_from_dict_no_defaults(self):

        class Child(base_model.BaseModel):
            i: int
            s: str
            f: float
            l: List[int]

        data = {'i': 1, 's': 's', 'f': 1.0, 'l': [1, 2]}
        child = Child.from_dict(data)
        self.assertEqual(child.i, 1)
        self.assertEqual(child.s, 's')
        self.assertEqual(child.f, 1.0)
        self.assertEqual(child.l, [1, 2])

    def test_from_dict_with_defaults(self):

        class Child(base_model.BaseModel):
            s: str
            f: float
            l: List[int]
            i: int = 1

        data = {'s': 's', 'f': 1.0, 'l': [1, 2]}
        child = Child.from_dict(data)
        self.assertEqual(child.i, 1)
        self.assertEqual(child.s, 's')
        self.assertEqual(child.f, 1.0)
        self.assertEqual(child.l, [1, 2])

    def test_from_dict_nested(self):

        class InnerChild(base_model.BaseModel):
            a: str

        class Child(base_model.BaseModel):
            b: int
            c: InnerChild

        data = {'b': 1, 'c': {'a': 'a'}}

        child = Child.from_dict(data)
        self.assertEqual(child.b, 1)
        self.assertIsInstance(child.c, InnerChild)
        self.assertEqual(child.c.a, 'a')

    def test_from_dict_array_nested(self):

        class InnerChild(base_model.BaseModel):
            a: str

        class Child(base_model.BaseModel):
            b: int
            c: List[InnerChild]
            d: Dict[str, InnerChild]

        inner_child_data = {'a': 'a'}
        data = {
            'b': 1,
            'c': [inner_child_data, inner_child_data],
            'd': {
                'e': inner_child_data
            }
        }

        child = Child.from_dict(data)
        self.assertEqual(child.b, 1)
        self.assertIsInstance(child.c[0], InnerChild)
        self.assertIsInstance(child.c[1], InnerChild)
        self.assertIsInstance(child.d['e'], InnerChild)
        self.assertEqual(child.c[0].a, 'a')

    def test_from_dict_camel(self):

        class InnerChild(base_model.BaseModel):
            inner_child_field: int

        class Child(base_model.BaseModel):
            sub_field: InnerChild

        data = {'subField': {'innerChildField': 2}}

        child = Child.from_dict(data)
        self.assertIsInstance(child.sub_field, InnerChild)
        self.assertEqual(child.sub_field.inner_child_field, 2)

    def test_to_dict_camel(self):

        class InnerChild(base_model.BaseModel):
            inner_child_field: int

        class Child(base_model.BaseModel):
            sub_field: InnerChild

        inner_child = InnerChild(inner_child_field=2)
        child = Child(sub_field=inner_child)
        actual = child.to_dict()
        expected = {'subField': {'innerChildField': 2}}
        self.assertEqual(actual, expected)

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

    def test_load_primitive(self):
        self.assertEqual(base_model._load_basemodel_helper(str, 'a'), 'a')
        self.assertEqual(base_model._load_basemodel_helper(int, 1), 1)
        self.assertEqual(base_model._load_basemodel_helper(float, 1.0), 1.0)
        self.assertEqual(base_model._load_basemodel_helper(bool, True), True)
        self.assertEqual(
            base_model._load_basemodel_helper(type(None), None), None)

    def test_load_primitive_with_casting(self):
        self.assertEqual(base_model._load_basemodel_helper(int, '1'), 1)
        self.assertEqual(base_model._load_basemodel_helper(str, 1), '1')
        self.assertEqual(base_model._load_basemodel_helper(float, 1), 1.0)
        self.assertEqual(base_model._load_basemodel_helper(int, 1.0), 1)
        self.assertEqual(base_model._load_basemodel_helper(bool, 1), True)
        self.assertEqual(base_model._load_basemodel_helper(bool, 0), False)
        self.assertEqual(base_model._load_basemodel_helper(int, True), 1)
        self.assertEqual(base_model._load_basemodel_helper(int, False), 0)
        self.assertEqual(base_model._load_basemodel_helper(bool, None), False)

    def test_load_none(self):
        self.assertEqual(
            base_model._load_basemodel_helper(type(None), None), None)
        with self.assertRaisesRegex(TypeError, ''):
            base_model._load_basemodel_helper(type(None), 1)

    @parameterized.parameters(['a', 1, 1.0, True, False, None, ['list']])
    def test_load_any(self, data: Any):
        self.assertEqual(base_model._load_basemodel_helper(Any, data),
                         data)  # type: ignore

    def test_load_list(self):
        self.assertEqual(
            base_model._load_basemodel_helper(List[str], ['a']), ['a'])
        self.assertEqual(
            base_model._load_basemodel_helper(List[int], [1, 1]), [1, 1])
        self.assertEqual(
            base_model._load_basemodel_helper(List[float], [1.0]), [1.0])
        self.assertEqual(
            base_model._load_basemodel_helper(List[bool], [True]), [True])
        self.assertEqual(
            base_model._load_basemodel_helper(List[type(None)], [None]), [None])

    def test_load_primitive_other_iterables(self):
        self.assertEqual(
            base_model._load_basemodel_helper(abc.Sequence[int], [1]), [1])
        self.assertEqual(
            base_model._load_basemodel_helper(abc.MutableSequence[float],
                                              [1.0]), [1.0])
        self.assertEqual(
            base_model._load_basemodel_helper(Sequence[bool], [True]), [True])
        self.assertEqual(
            base_model._load_basemodel_helper(MutableSequence[type(None)],
                                              [None]), [None])
        self.assertEqual(
            base_model._load_basemodel_helper(Sequence[str], ['a']), ['a'])

    def test_load_dict(self):
        self.assertEqual(
            base_model._load_basemodel_helper(Dict[str, str], {'a': 'a'}),
            {'a': 'a'})
        self.assertEqual(
            base_model._load_basemodel_helper(Dict[str, int], {'a': 1}),
            {'a': 1})
        self.assertEqual(
            base_model._load_basemodel_helper(Dict[str, float], {'a': 1.0}),
            {'a': 1.0})
        self.assertEqual(
            base_model._load_basemodel_helper(Dict[str, bool], {'a': True}),
            {'a': True})
        self.assertEqual(
            base_model._load_basemodel_helper(Dict[str, type(None)],
                                              {'a': None}), {'a': None})

    def test_load_mapping(self):
        self.assertEqual(
            base_model._load_basemodel_helper(abc.Mapping[str, str],
                                              {'a': 'a'}), {'a': 'a'})
        self.assertEqual(
            base_model._load_basemodel_helper(abc.MutableMapping[str, int],
                                              {'a': 1}), {'a': 1})
        self.assertEqual(
            base_model._load_basemodel_helper(Mapping[str, float], {'a': 1.0}),
            {'a': 1.0})
        self.assertEqual(
            base_model._load_basemodel_helper(MutableMapping[str, bool],
                                              {'a': True}), {'a': True})
        self.assertEqual(
            base_model._load_basemodel_helper(OrderedDict[str, type(None)],
                                              {'a': None}), {'a': None})

    def test_load_union_types(self):
        self.assertEqual(
            base_model._load_basemodel_helper(Union[str, int], 'a'), 'a')
        self.assertEqual(
            base_model._load_basemodel_helper(Union[str, int], 1), '1')
        self.assertEqual(
            base_model._load_basemodel_helper(Union[int, str], 1), 1)
        self.assertEqual(
            base_model._load_basemodel_helper(Union[int, str], '1'), 1)

    def test_load_optional_types(self):
        self.assertEqual(
            base_model._load_basemodel_helper(Optional[str], 'a'), 'a')
        self.assertEqual(
            base_model._load_basemodel_helper(Optional[str], None), None)

    def test_unsupported_type(self):
        with self.assertRaisesRegex(TypeError, r'Unsupported type:'):
            base_model._load_basemodel_helper(Set[int], {1})


class TestGetOriginPy37(parameterized.TestCase):

    def test_is_same_as_typing_version(self):
        import sys
        if sys.version_info.major == 3 and sys.version_info.minor >= 8:
            import typing
            for field in TypeClass.fields:
                self.assertEqual(
                    base_model._get_origin_py37(field.type),
                    typing.get_origin(field.type))


class TestGetArgsPy37(parameterized.TestCase):

    def test_is_same_as_typing_version(self):
        import sys
        if sys.version_info.major == 3 and sys.version_info.minor >= 8:
            import typing
            for field in TypeClass.fields:
                self.assertEqual(
                    base_model._get_args_py37(field.type),
                    typing.get_args(field.type))


if __name__ == '__main__':
    unittest.main()

# # TODO: test no subtype
# # TODO: adjust special types to actually use that data structure
