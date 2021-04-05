import datetime

import pytest

from apache_beam.io.gcp.datastore.v1new.types import Entity, Key

from . import datastore


class EntityWithProps(Entity):
    def __init__(self, key, properties=None):
        super().__init__(key)
        if properties is not None:
            self.properties = properties


@pytest.mark.parametrize('entity,expected_kind,expected_id', [
    (EntityWithProps(Key(path_elements=['a', 'b', 'c', 'd'])), 'c', 'd'),
    (EntityWithProps(Key(path_elements=['a', 'b'])), 'a', 'b')
])
def test_get_kind(entity, expected_kind, expected_id):
    kind = datastore.get_kind(entity)

    assert kind.name == expected_kind
    assert kind.id == expected_id


@pytest.mark.parametrize('entity,expected_dict', [
    (EntityWithProps(Key(path_elements=['a', 'b'], namespace='ns1')), {
        '__key__': {
            'kind': 'a',
            'id': None,
            'name': 'b',
            'namespace': 'ns1',
            'path': 'a/b'
        }
    }),
    (EntityWithProps(Key(path_elements=['a', 12345], namespace='ns1'), {
        'key1': 'val1',
        'key2': 123,
        'key3': datetime.datetime(2020, 1, 2, 3, 4, 5)
    }), {
        '__key__': {
            'kind': 'a',
            'id': 12345,
            'name': None,
            'namespace': 'ns1',
            'path': 'a/12345',
        },
        'key1': 'val1',
        'key2': 123,
        'key3': '2020-01-02 03:04:05'
    })
])
def test_entity_to_json(entity, expected_dict):
    entity_json = datastore.entity_to_json(entity)

    assert entity_json == expected_dict


@pytest.mark.parametrize('all_kinds,ignore_names,expected_kinds', [
    (['Kind1', 'Kind2'], [], ['Kind1', 'Kind2']),
    (['Kind1', 'Kind2', 'OutKind'], ['Out'], ['Kind1', 'Kind2']),
    ([], [], []),
    (['OutKind1', 'Kind2', 'Kind3', 'NotKind4', 'Kind5', 'Kind6Not'], ['Out', 'Not'], ['Kind2', 'Kind3', 'Kind5', 'Kind6Not'])
])
def test_filterout_kinds(all_kinds, ignore_names, expected_kinds):
    kinds = datastore.filterout_kinds(all_kinds, ignore_names)

    assert kinds == expected_kinds