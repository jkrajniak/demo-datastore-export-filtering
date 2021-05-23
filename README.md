# Datastore to BigQuery with filtering using Dataflow

A demo code for the article https://towardsdatascience.com/export-datastore-to-bigquery-using-google-dataflow-1801c25ae482

# How to run

First, initialize output BigQuery dataset and temporary Cloud Storage

```sh
$ make init PROJECT_ID=<gcp project id>
```

then you can run the pipeline

- `$ make run-local PROJECT_ID=<gcp project id>` to run locally
- `$ make run-dataflow PROJECT_ID=<gcp project id>` to run on GCP
