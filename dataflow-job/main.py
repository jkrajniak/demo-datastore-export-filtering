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


def run(argv=None):
    """Main entry point to the pipeline."""

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--conf',
        dest='conf',
        required=True,
        default='conf.yaml')

    known_args, pipeline_args = parser.parse_known_args(argv)

    conf = yaml.load(open(known_args.conf, 'r'), Loader=yaml.SafeLoader)

    output_dataset = conf['OutputDataset']
    entity_filtering = get_filter_entities_from_conf(conf['KindsToExport'])
    prefix_of_kinds_to_ignore = conf['PrefixOfKindsToIgnore']

    pipeline_options = PipelineOptions(pipeline_args)
    project_id = pipeline_options.view_as(GoogleCloudOptions).project

    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).setup_file = './setup.py'
    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).save_main_session = True
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = "us-east1"
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).machine_type = 'n1-standard-1'
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).num_workers = 2
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).disk_size_gb = 25
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).autoscaling_algorithm = 'NONE'

    gcs_dir = "gs://{}-dataflow/temp/{}".format(project_id, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

    class TagElementsWithData(beam.DoFn):
        def process(self, element):
            tag = 'write_truncate'
            if element['__key__']['kind'] in conf['KindsToExport']:
                tag = 'write_append'
            yield TaggedOutput(tag, element)

    with beam.Pipeline(options=pipeline_options) as p:
        # Create a query and filter by ktable_side_inputsind
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
