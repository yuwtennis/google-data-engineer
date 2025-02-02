REGION := $(shell gcloud config get compute/region)
PROJECT_ID := $(shell gcloud config get core/project)
BUCKET_NAME ?= $(PROJECT_ID)-jp
TRAIN_DATA ?= /gcs/$(BUCKET_NAME)/flights/train.csv
EVAL_DATA ?= /gcs/$(BUCKET_NAME)/flights/test.csv
OUTPUT_DIR ?= /gcs/$(BUCKET_NAME)/flights/output/
COMMA := ,
TRAIN_BATCH_SIZE ?= 64
NUM_OF_BUCKETS ?= 5
REPOS_NAME ?= custom-training
TAG ?= $(shell git rev-parse HEAD)
CONTAINER_IMAGE_URI ?= $(REGION)-docker.pkg.dev\/$(PROJECT_ID)\/$(REPOS_NAME)\/flights:$(TAG)
MACHINE_TYPE ?= n2-standard-4
FUNC ?= embeddings

build:
	docker build -t $(CONTAINER_IMAGE_URI) .

push:
	docker push $(CONTAINER_IMAGE_URI)

train:
	gcloud ai custom-jobs create \
	  --region=asia-northeast1 \
	  --display-name=flights \
	  --args=--traindata="$(TRAIN_DATA)",--evaldata="$(EVAL_DATA)",--output="$(OUTPUT_DIR)",--func="$(FUNC)",--train_batch_size=$(TRAIN_BATCH_SIZE),--num_of_buckets=$(NUM_OF_BUCKETS),--dnn_hidden_units="64-16-4" \
	  --worker-pool-spec=replica-count=1,machine-type=$(MACHINE_TYPE),container-image-uri=$(CONTAINER_IMAGE_URI)

test:
	pylint trainer/
	mypy trainer/

hp-tuning:
	cp hyperparameter.yaml hyperparameter.yaml.tmp ; \
	sed -i "s/CONTAINER_IMAGE_URI/$(CONTAINER_IMAGE_URI)/"  hyperparameter.yaml.tmp ; \
	gcloud ai hp-tuning-jobs create \
		--region=asia-northeast1 \
		--display-name=flights \
		--config=hyperparameter.yaml.tmp ; \
	rm -v hyperparameter.yaml.tmp

import-model:
	gcloud ai models upload \
		--region=asia-northeast1 \
		--display-name=flights \
		--container-image-uri=asia-docker.pkg.dev/vertex-ai-restricted/prediction/tf_opt-cpu.2-14:latest \
		--artifact-uri=$(GS_ARTIFACT_URI)

deploy:
	gcloud ai endpoints deploy-model $(ENDPOINT) \
		--region=asia-northeast1 \
		--display-name=flights \
		--model=$(MODEL_ID) \
