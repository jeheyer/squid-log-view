SERVICE = squid-log-view
RUNTIME := python311
REGION := us-central1

include Makefile.env

all: gcp-setup cloud-function cloud-run

gcp-setup:
	gcloud config set project $(PROJECT_ID)

app-engine:
	gcloud app deploy

cloud-function:
	gcloud config set functions/region $(REGION)
	gcloud functions deploy $(SERVICE) --runtime=$(RUNTIME) --region=$(REGION) \
	--gen2 --source=. --entry-point=ping --trigger-http --memory=512MB --allow-unauthenticated

cloud-run:
	gcloud config set run/region $(REGION)
	gcloud builds submit --tag gcr.io/$(PROJECT_ID)/$(SERVICE) .
	gcloud run deploy $(SERVICE) --image gcr.io/$(PROJECT_ID)/$(SERVICE) --allow-unauthenticated

