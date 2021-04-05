PROJECT_ID?=
STAGE?=dev

GS_TEMP:=gs://$(PROJECT_ID)-dataflow-$(STAGE)

CMD_PIPELINE:=python main.py --project $(PROJECT_ID) --temp_location $(GS_TEMP)/temp --conf conf.yaml

OUTPUT_DATASET?=datastore_$(STAGE)

init:
	gsutil mb $(GS_TEMP) || echo "Bucket exists"
	bq --project_id $(PROJECT_ID) mk $(OUTPUT_DATASET) || echo "Dataset exists"


run-local:
	$(CMD_PIPELINE) --runner direct --gcs_dir $(GS_TEMP) --dataset $(OUTPUT_DATASET)

run-dataflow:
	$(CMD_PIPELINE) --runner dataflow --gcs_dir $(GS_TEMP) --dataset $(OUTPUT_DATASET)
