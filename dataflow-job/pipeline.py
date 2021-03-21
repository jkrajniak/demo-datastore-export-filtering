import argparse
import datetime
import logging
import sys

import apache_beam as beam
import yaml
from apache_beam import pvalue
from apache_beam.io.gcp.bigquery_file_loads import BigQueryBatchFileLoads
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions

from helpers import entity_to_json, GetKinds, CreateQuery, get_filter_entities_from_conf


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

    # pipeline_options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(beam.options.pipeline_options.SetupOptions).setup_file = './setup.py'
    pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = "us-east1"
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).num_workers = 1
    pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).disk_size_gb = 25
    # pipeline_options.view_as(beam.options.pipeline_options.WorkerOptions).use_public_ips = False

    if project_id is None:
        parser.print_usage()
        print(sys.argv[0] + ': error: argument --project_id is required')
        sys.exit(1)

    gcs_dir = "gs://{}-dataflow/temp/{}".format(project_id, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))

    with beam.Pipeline(options=pipeline_options) as p:
        # Create a query and filter by kind
        rows = (p
                | 'get all kinds' >> GetKinds(project_id, prefix_of_kinds_to_ignore)
                | 'create queries' >> beam.ParDo(CreateQuery(project_id, entity_filtering))
                | 'read from datastore' >> beam.ParDo(ReadFromDatastore._QueryFn())
                | 'convert entities' >> beam.Map(entity_to_json)
                | 'split entities' >> beam.Map(lambda el: pvalue.TaggedOutput('write_append' if el['__key__']['kind'] in conf['KindsToExport'] else 'write_truncate', el)).with_outputs()
                )

        write_append = rows.write_append
        write_truncate = rows.write_truncated

        _ = write_append | 'write append' >> BigQueryBatchFileLoads(
            destination=lambda row: f'{project_id}:{output_dataset}.{row["__key__"]["kind"]}',
            custom_gcs_temp_location=gcs_dir,
            write_disposition='WRITE_APPEND',
            create_disposition='CREATE_IF_NEEDED',
            schema='SCHEMA_AUTODETECT')
        _ = write_truncate | 'write truncate' >> BigQueryBatchFileLoads(
            destination=lambda row: f'{project_id}:{output_dataset}.{row["__key__"]["kind"]}',
            custom_gcs_temp_location=gcs_dir,
            write_disposition='WRITE_TRUNCATED',
            create_disposition='CREATE_IF_NEEDED',
            schema='SCHEMA_AUTODETECT')


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
