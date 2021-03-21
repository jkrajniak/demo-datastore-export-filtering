import datetime
import logging
from typing import Dict

from apache_beam import Create, DoFn
from apache_beam import PTransform
from apache_beam.io.gcp.datastore.v1new.types import Query
from apache_beam.transforms import Impulse
from google.cloud import datastore
from google.cloud.datastore.helpers import GeoPoint
from pytimeparse.timeparse import timeparse


class Kind:
    def __init__(self, name, kind_id):
        self.name = name
        self.id = kind_id


def get_kind(entity):
    if len(entity.key.path_elements) > 2:
        kind = entity.key.path_elements[2]
        id_or_name = entity.key.path_elements[3]
    else:
        kind = entity.key.path_elements[0]
        id_or_name = entity.key.path_elements[1]

    return Kind(kind, id_or_name)


def entity_to_json(entity):
    kind = get_kind(entity)

    entity_dict = {
        '__key__': {
            'name': isinstance(kind.id, str) and kind.id or None,
            'id': isinstance(kind.id, int) and kind.id or None,
            'kind': kind.name,
            'namespace': entity.key.namespace,
            'path': ','.join([isinstance(e, str) and '"{}"'.format(e) or str(e) for e in entity.key.path_elements])
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


class GetKinds(PTransform):
    """
    Get all Kind names
    """

    def __init__(self, project_id, prefix_of_kinds_to_ignore):
        super().__init__()
        self.project_id = project_id
        self.prefix_of_kinds_to_ignore = prefix_of_kinds_to_ignore

    def expand(self, pcoll):
        """
        :return: PCollection[kind_name]
        """

        query = datastore.Client(self.project_id).query(kind='__kind__')
        query.keys_only()
        all_kinds = [entity.key.id_or_name for entity in query.fetch()]

        kinds = []
        for kind_name in all_kinds:
            ignore_kind = False
            for kind_prefix in self.prefix_of_kinds_to_ignore:
                if kind_name.startswith(kind_prefix):
                    ignore_kind = True
                    break
            if not ignore_kind:
                kinds.append(kind_name)


        logging.info("kinds: {}".format(kinds))
        return pcoll.pipeline | 'Kind' >> Create(kinds)


class FilterEntity:
    def __init__(self, field_name, end_time, time_interval):
        self.field_name = field_name
        self.end_time = tuple(map(int, end_time.split(':')))
        self.time_interval = datetime.timedelta(seconds=timeparse(time_interval))

    def get_filter(self) -> tuple:
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


def get_filter_entities_from_conf(param) -> Dict[str, FilterEntity]:
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

    def __init__(self, project_id: str, entity_filtering: Dict[str, FilterEntity], *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.project_id = project_id
        self.entity_filtering = entity_filtering

    def process(self, element, **kwargs):
        """
        :param element: a kind name
        :return: [Query]
        """

        q = Query(kind=element, project=self.project_id)
        if element in self.entity_filtering:
            q.filters = self.entity_filtering[element].get_filter()

        logging.info(f'Query for kind {element}: {q}')

        return [q]


class GetBqTableMap(PTransform):
    """
    Get table information of BigQuery as dictionary
    """

    def __init__(self, project_id, dataset_opt):
        super().__init__()
        self.project_id = project_id
        self.dataset_opt = dataset_opt

    def expand(self, pbegin):
        """
        :return: PCollection[{"kind_name": "bigquery_table_name"}]
        """

        query = datastore.Client(self.project_id).query(kind='__kind__')
        query.keys_only()
        kinds = [entity.key.id_or_name for entity in query.fetch()]
        return (pbegin
                | Impulse()
                | beam.FlatMap(lambda _: [(kind, "{}.{}".format(self.dataset_opt.get(), kind)) for kind in kinds])
                )
