import datetime
import logging
from typing import Dict, List

from apache_beam import PTransform, Create, DoFn
from google.cloud import datastore
from apache_beam.io.gcp.datastore.v1new.types import Query, Entity
from google.cloud.datastore.helpers import GeoPoint
from pytimeparse.timeparse import timeparse


class Kind:
    """Represents kind"""

    def __init__(self, name, kind_id):
        self.name = name
        self.id = kind_id


def get_kind(entity: Entity) -> Kind:
    """
    Get kind from entity
    :param entity: Datastore entity
    :return: Kind
    """

    if len(entity.key.path_elements) > 2:
        kind = entity.key.path_elements[2]
        id_or_name = entity.key.path_elements[3]
    else:
        kind = entity.key.path_elements[0]
        id_or_name = entity.key.path_elements[1]

    return Kind(kind, id_or_name)


def entity_to_json(entity):
    """
    Convert datastore entity to JSON

    :param entity: datastore entity
    :return: dictionary with the entity
    """
    kind = get_kind(entity)

    entity_dict = {
        '__key__': {
            'name': isinstance(kind.id, str) and kind.id or None,
            'id': isinstance(kind.id, int) and kind.id or None,
            'kind': kind.name,
            'namespace': entity.key.namespace,
            'path': '/'.join(map(str, entity.key.path_elements))
        }
    }

    for k, v in entity.properties.items():
        if isinstance(v, datetime.datetime):
            entity_dict[k] = str(v)
        elif isinstance(v, GeoPoint):
            entity_dict[k] = {'lat': str(v.latitude), 'lng': str(v.longitude)}
        else:
            entity_dict[k] = v

    return entity_dict


def filterout_kinds(all_kinds: List[str], kind_prefix_to_ignore: List[str] = None) -> List[str]:
    if kind_prefix_to_ignore is None:
        return all_kinds

    kinds = []
    for kind_name in all_kinds:
        ignore_kind = False
        for name_prefix in kind_prefix_to_ignore:
            if kind_name.startswith(name_prefix):
                ignore_kind = True
                break
        if not ignore_kind:
            kinds.append(kind_name)
    return kinds


class GetAllKinds(PTransform):
    """
    Get kinds from all namespaces.
    """

    def __init__(self, project_id: str, prefix_of_kinds_to_ignore: list):
        """
        :param project_id: The project id.
        :param prefix_of_kinds_to_ignore: The list of kind prefixes to be ignored.
        """
        super().__init__()
        self.project_id = project_id
        self.prefix_of_kinds_to_ignore = prefix_of_kinds_to_ignore

    def expand(self, pcoll):
        """
        :return: PCollection[kind_name]
        """

        # Get all kinds.
        query = datastore.Client(self.project_id).query(kind='__kind__')
        query.keys_only()
        all_kinds = [entity.key.id_or_name for entity in query.fetch()]

        kinds = filterout_kinds(all_kinds, self.prefix_of_kinds_to_ignore)

        logging.info("kinds: {}".format(kinds))
        return pcoll.pipeline | 'Kind' >> Create(kinds)


class FilterEntity:
    """FilterEntity keeps information about the filtering of given kind."""

    def __init__(self, field_name: str, end_time: str, time_interval: str):
        """

        :param field_name: Field name to use for filtering.
        :param end_time: The end time use to filtering
        :param time_interval: The string with time interval.
        """
        self.field_name = field_name
        self.end_time = tuple(map(int, end_time.split(':')))
        self.time_interval = datetime.timedelta(seconds=timeparse(time_interval))

    def get_filter(self) -> tuple:
        """Get filter configuration"""
        hour = self.end_time[0]
        minute = self.end_time[1]
        second = 0
        if len(self.end_time) == 3:
            second = self.end_time[2]

        end_date = datetime.datetime.now().replace(hour=hour, minute=minute, second=second)
        start_date = end_date - self.time_interval
        return (
            (self.field_name, '>=', start_date),
            (self.field_name, '<', end_date)
        )


def get_filter_entities_from_conf(param: dict) -> Dict[str, FilterEntity]:
    """
    Get the entity filters from parameter dictionary.
    :param param:
    :return:
    """
    filter_entities = {}
    for kind_name, kind_conf in param.items():
        filter_entities[kind_name] = FilterEntity(
            kind_conf['field'],
            kind_conf['endTime'],
            kind_conf['timeInterval']
        )
    return filter_entities


class CreateQuery(DoFn):
    """
    Create a query for getting all entities the kind taken.
    """

    def __init__(self, project_id: str, entity_filtering: Dict[str, FilterEntity]):
        """
        :param project_id: GCP Project id
        :param entity_filtering: Dictionary with filtering options for given kind.
        """
        self.project_id = project_id
        self.entity_filtering = entity_filtering

    def process(self, kind_name, **kwargs):
        """
        :param **kwargs:
        :param kind_name: a kind name
        :return: [Query]
        """

        logging.info(f'CreateQuery.process {kind_name} {kwargs}')

        q = Query(kind=kind_name, project=self.project_id)
        if kind_name in self.entity_filtering:
            q.filters = self.entity_filtering[kind_name].get_filter()

        logging.info(f'Query for kind {kind_name}: {q}')

        yield q
