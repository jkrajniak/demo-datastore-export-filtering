import argparse
import datetime
import logging

import apache_beam as beam
import yaml
from apache_beam import TaggedOutput
from apache_beam.io.gcp.bigquery_file_loads import BigQueryBatchFileLoads
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions

from transform.datastore import entity_to_json, GetAllKinds, CreateQuery, get_filter_entities_from_conf


class CustomPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--conf',
            dest='conf',
            required=True,
            default='conf.yaml')
        parser.add_argument(
            '--gcs_dir',
            dest='gcs_dir',
            required=True)
        parser.add_argument(
            '--dataset',
            dest='dataset',
            required=True,
            help='Name of the output dataset in BigQuery')
        parser.add_argument(
            '--project_ids',
            required=False,
            help='Comma separated list of GCP projects'
        )


def run(argv=None):
    """Main entry point to the pipeline."""

    pipeline_options = CustomPipelineOptions(argv)

    output_dataset = pipeline_options.dataset

    conf = yaml.load(open(pipeline_options.conf, 'r'), Loader=yaml.SafeLoader)
    entity_filtering = get_filter_entities_from_conf(conf['KindsToExport'])
    prefix_of_kinds_to_ignore = conf['PrefixOfKindsToIgnore']
    
    if pipeline_options.project_ids is None:
        project_ids = [pipeline_options.view_as(GoogleCloudOptions).project]
    else:
        project_ids = list(map(str.strip, pipeline_options.project_ids.split(',')))

    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).setup_file = './setup.py'
    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = "us-east1"
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).machine_type = 'n1-standard-1'
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).num_workers = 2
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).disk_size_gb = 25
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).autoscaling_algorithm = 'NONE'

    gcs_dir = f'{pipeline_options.gcs_dir}/temp/{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}'

    class TagElementsWithData(beam.DoFn):
        def process(self, element):
            tag = 'write_truncate'
            if element['__key__']['kind'] in conf['KindsToExport']:
                tag = 'write_append'
            yield TaggedOutput(tag, element)

    with beam.Pipeline(options=pipeline_options) as p:
        # Create a query and filter
        for project_id in project_ids:
            rows = (p
                    | 'get all kinds' >> GetAllKinds(project_id, prefix_of_kinds_to_ignore)
                    | 'create queries' >> beam.ParDo(CreateQuery(project_id, entity_filtering))
                    | 'read from datastore' >> beam.ParDo(ReadFromDatastore._QueryFn())
                    | 'convert entities' >> beam.Map(entity_to_json)
                    )

            tagged_data = rows | 'split entities' >> beam.ParDo(TagElementsWithData()).with_outputs()

            write_append = tagged_data.write_append
            write_truncate = tagged_data.write_truncate

            # Write entities that are after filtering.
            _ = write_append | 'write append' >> BigQueryBatchFileLoads(
                destination=lambda row: f"{project_id}:{output_dataset}.{row['__key__']['kind'].lower()}",
                custom_gcs_temp_location=f'{gcs_dir}/append',
                write_disposition='WRITE_APPEND',
                create_disposition='CREATE_IF_NEEDED',
                schema='SCHEMA_AUTODETECT')

            # Write the kinds that are not filtered - full load mode.
            _ = write_truncate | 'write truncate' >> BigQueryBatchFileLoads(
                destination=lambda row: f"{project_id}:{output_dataset}.{row['__key__']['kind'].lower()}",
                custom_gcs_temp_location=f'{gcs_dir}/truncate',
                write_disposition='WRITE_TRUNCATE',
                create_disposition='CREATE_IF_NEEDED',
                schema='SCHEMA_AUTODETECT')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
