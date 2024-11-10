REGION := $$(gcloud config get compute/region)
PROJECT_ID := $$(gcloud config get core/project)
BUCKET_NAME ?= $(PROJECT_ID)-jp
TRAIN_DATA ?= /gcs/$(BUCKET_NAME)/flights/train.csv
EVAL_DATA ?= /gcs/$(BUCKET_NAME)/flights/test.csv
OUTPUT_DIR ?= /gcs/$(BUCKET_NAME)/flights/output/
REPOS_NAME ?= custom-training
TAG ?= $$(git rev-parse HEAD)
CONTAINER_IMAGE_URI ?= $(REGION)-docker.pkg.dev/$(PROJECT_ID)/$(REPOS_NAME)/flights:$(TAG)
MACHINE_TYPE ?= n2-standard-4
FUNC ?= embeddings

build:
	docker build -t $(CONTAINER_IMAGE_URI) .

push:
	docker push $(CONTAINER_IMAGE_URI)

deploy:
	gcloud ai custom-jobs create \
	  --region=asia-northeast1 \
	  --display-name=flights \
	  --args=--traindata="$(TRAIN_DATA)",--evaldata="$(EVAL_DATA)",--output="$(OUTPUT_DIR)",--func="$(FUNC)" \
	  --worker-pool-spec=replica-count=1,machine-type=$(MACHINE_TYPE),container-image-uri=$(CONTAINER_IMAGE_URI)

test:
	pylint trainer/
	mypy trainer/