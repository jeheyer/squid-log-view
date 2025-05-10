SERVICE = squid-log-view
HOST := us-docker.pkg.dev
REPO := cloudbuild
RUNTIME := python313
REGION := us-central1
PORT := 8080

include Makefile.env

IMAGE = $(HOST)/$(PROJECT_ID)/$(REPO)/$(SERVICE):latest

all: docker gcp
docker: docker-build docker-run
gcp: gcp-setup app-engine cloud-function cloud-run

docker-build:
	docker build -t $(SERVICE) .

docker-run:
	docker run -p 31280\:$(PORT) $(SERVICE)

gcp-setup:
	gcloud config set project $(PROJECT_ID)
	gcloud config set core/project $(PROJECT_ID)
	gcloud config set compute/region $(REGION)

app-engine:
	gcloud app deploy ./app.yaml

cloud-function:
	gcloud config set functions/region $(REGION)
	gcloud functions deploy $(SERVICE) --runtime=$(RUNTIME) --region=$(REGION) \
	--gen2 --source=. --entry-point=ping --trigger-http --memory=512MB --allow-unauthenticated

cloud-build:
	#gcloud builds submit --tag gcr.io/$(PROJECT_ID)/$(SERVICE) .
	gcloud builds submit --tag $(IMAGE) .

cloud-run:
	gcloud config set run/region $(REGION)
	#gcloud run deploy $(SERVICE) --image gcr.io/$(PROJECT_ID)/$(SERVICE) --port $(PORT) --allow-unauthenticated
	gcloud run deploy $(SERVICE) --image $(IMAGE) --port $(PORT) --platform=managed --allow-unauthenticated
