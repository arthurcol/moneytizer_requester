clean_container:
	@docker kill $(CONTAINER_NAME)
	@docker rm $(CONTAINER_NAME)

build_image:
	@docker build -t eu.gcr.io/whaly-x-the-moneytizer/reketor:dev .

run_image:
	@docker run -p 8080:8080 --env-file .env --name=$(CONTAINER_NAME) eu.gcr.io/whaly-x-the-moneytizer/reketor:dev

build_prod:
	@docker build --platform=amd64 -t eu.gcr.io/whaly-x-the-moneytizer/reketor:prod .

push_prod:
	@docker push eu.gcr.io/whaly-x-the-moneytizer/reketor:prod

deploy_prod:
	@gcloud run deploy --image eu.gcr.io/whaly-x-the-moneytizer/reketor:prod \
										--region europe-west1 \
										--memory 2Gi \
										--env-vars-file .env.yaml \
										--project $(PROJECT) \
										--service-account $(SERVICE_ACCOUNT)

restart:
	@make clean_container
	@make build_image
	@make run_image
