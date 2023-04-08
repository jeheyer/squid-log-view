CLOUD_RUN_REGION := us-central1

include Makefile.env

all: gcp-setup cloud-build cloud-run-deploy

gcp-setup:
	gcloud config set project $(PROJECT_ID)

cloud-build:
	gcloud builds submit --tag gcr.io/$(PROJECT_ID)/$(SERVICE_NAME) .

cloud-run-deploy:
	gcloud config set run/region $(CLOUD_RUN_REGION)
	gcloud run deploy $(SERVICE_NAME) --image gcr.io/$(PROJECT_ID)/$(SERVICE_NAME) --allow-unauthenticated

