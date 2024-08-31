SERVICE_NAME := squid-log-view
RUNTIME := python311
REGION := us-central1

include Makefile.env

all: gcp-setup cloud-run

gcp-setup:
	gcloud config set project $(GCP_PROJECT_ID)

cloud-function:
	gcloud config set functions/region $(REGION)
	gcloud functions deploy $(SERVICE_NAME) --runtime=$(RUNTIME)  --region=$(REGION) \
	--gen2 --source=. --entry-point=hello_get --trigger-http --allow-unauthenticated

cloud-run:
	gcloud config set run/region $(REGION)
	gcloud builds submit --tag gcr.io/$(GCP_PROJECT_ID)/$(SERVICE_NAME) .
	gcloud run deploy $(SERVICE_NAME) --image gcr.io/$(GCP_PROJECT_ID)/$(SERVICE_NAME) --allow-unauthenticated

