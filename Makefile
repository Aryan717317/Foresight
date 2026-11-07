DOCKERHUB_USER ?= your-dockerhub-username
APP_IMAGE      := $(DOCKERHUB_USER)/self-healing-eks-app
UI_IMAGE       := $(DOCKERHUB_USER)/self-healing-eks-ui
BOT_IMAGE      := $(DOCKERHUB_USER)/self-healing-eks-bot
ML_IMAGE       := $(DOCKERHUB_USER)/self-healing-eks-ml
PRODUCER_IMAGE := $(DOCKERHUB_USER)/self-healing-eks-producer
CONSUMER_IMAGE := $(DOCKERHUB_USER)/self-healing-eks-consumer
TAG            := latest

# Suppress minikube emoji / colour output for clean CI/terminal logs.
export MINIKUBE_IN_STYLE := false

.PHONY: help
help:
	@echo ""
	@echo "Self-Healing EKS Command Interface"
	@echo "=================================="
	@echo ""
	@echo "  CLUSTER SETUP"
	@echo "  make minikube-start          Start minikube with required resources"
	@echo "  make install-ingress         Install nginx ingress controller"
	@echo "  make install-monitoring      Install Prometheus + Grafana stack"
	@echo "  make install-kafka           Install Kafka (KRaft, single-broker)"
	@echo "  make install-mlflow          Deploy MLflow tracking server"
	@echo "  make install-all             Full setup from scratch (reactive + predictive)"
	@echo ""
	@echo "  APP"
	@echo "  make build                   Build app Docker image locally"
	@echo "  make push                    Push image to Docker Hub"
	@echo "  make deploy-app              Deploy app + postgres to minikube"
	@echo "  make deploy-bot              Deploy remediation bot"
	@echo "  make deploy-monitoring       Apply alert rules + servicemonitor"
	@echo ""
	@echo "  PREDICTIVE PLATFORM"
	@echo "  make deploy-streaming        Deploy Kafka producer + LSTM consumer"
	@echo "  make build-streaming         Build producer + consumer images (minikube)"
	@echo "  make train-model             Generate synthetic data + train LSTM locally"
	@echo "  make train-model-synthetic   Train on synthetic data (no Prometheus needed)"
	@echo "  make check-drift             Run drift detection (requires training_data.csv)"
	@echo "  make simulate-predictive     End-to-end demo: load → watch predictor fire"
	@echo ""
	@echo "  DEMO / TESTING"
	@echo "  make simulate-crash          Hit /crash endpoint (triggers CrashLoopBackOff)"
	@echo "  make simulate-load           Hit /load endpoint (triggers HighCPU alert)"
	@echo "  make watch-pods              Watch pod status in real-time"
	@echo "  make watch-hpa               Watch HPA scaling in real-time"
	@echo "  make watch-predictions       Tail predictor consumer logs"
	@echo ""
	@echo "  ACCESS"
	@echo "  make port-app                Forward app to localhost:8000"
	@echo "  make port-grafana            Forward Grafana to localhost:3000"
	@echo "  make port-prometheus         Forward Prometheus to localhost:9090"
	@echo "  make port-alertmanager       Forward Alertmanager to localhost:9093"
	@echo "  make port-bot                Forward remediation bot to localhost:9000"
	@echo "  make port-mlflow             Forward MLflow UI to localhost:5000"
	@echo ""
	@echo "  LOGS"
	@echo "  make logs-app                Stream app pod logs"
	@echo "  make logs-bot                Stream remediation bot logs"
	@echo "  make logs-producer           Stream Kafka producer logs"
	@echo "  make logs-consumer           Stream LSTM consumer logs"
	@echo ""
	@echo "  TERRAFORM"
	@echo "  make tf-init                 terraform init"
	@echo "  make tf-validate             terraform fmt + validate"
	@echo "  make tf-plan                 terraform plan"
	@echo ""
	@echo "  CLEANUP"
	@echo "  make clean                   Remove all deployed resources"
	@echo "  make minikube-stop           Stop minikube"
	@echo "  make minikube-delete         Delete minikube cluster completely"
	@echo ""


# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------

.PHONY: check-docker
check-docker:
	@docker info > /dev/null 2>&1 || { \
		echo "ERROR: Docker daemon is not running."; \
		echo "       Start Docker Desktop (or your Docker daemon) and retry."; \
		exit 1; \
	}

# ---------------------------------------------------------------------------
# Cluster setup
# ---------------------------------------------------------------------------

.PHONY: minikube-start
minikube-start: check-docker
	@echo "Starting minikube..."
	minikube start \
		--cpus=4 \
		--memory=6144 \
		--disk-size=20g \
		--driver=docker \
		--kubernetes-version=v1.35.1
	minikube addons enable metrics-server
	@echo "minikube ready."

.PHONY: install-ingress
install-ingress:
	@echo "Enabling ingress addon..."
	minikube addons enable ingress
	kubectl wait --namespace ingress-nginx \
		--for=condition=ready pod \
		--selector=app.kubernetes.io/component=controller \
		--timeout=180s || true
	@echo "Ingress controller ready."

.PHONY: install-monitoring
install-monitoring:
	@echo "Installing kube-prometheus-stack..."
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
	helm repo update
	helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
		--namespace monitoring \
		--create-namespace \
		--values helm/prometheus-values.yaml \
		--wait \
		--timeout 8m
	@echo "kube-prometheus-stack installed."

.PHONY: install-kafka
install-kafka:
	@echo "Deploying Kafka (KRaft mode, single broker)..."
	kubectl apply -f k8s/kafka.yaml
	kubectl rollout status statefulset/kafka -n monitoring --timeout=120s
	@echo "Kafka ready."

.PHONY: install-mlflow
install-mlflow:
	@echo "Deploying MLflow tracking server..."
	kubectl apply -f k8s/mlflow.yaml
	kubectl rollout status deployment/mlflow -n monitoring --timeout=90s
	@echo "MLflow ready. Run 'make port-mlflow' to access the UI."

.PHONY: install-all
install-all: minikube-start install-ingress install-monitoring install-kafka install-mlflow deploy-app deploy-bot deploy-monitoring deploy-streaming
	@echo ""
	@echo "Stack deployed (reactive + predictive)."
	@echo ""
	@echo "Next steps:"
	@echo "  make train-model-synthetic  ->  bootstrap the LSTM model"
	@echo "  make port-mlflow            ->  MLflow UI (model registry)"
	@echo "  make port-grafana           ->  Grafana dashboard"
	@echo "  make simulate-predictive    ->  end-to-end predictive demo"


.PHONY: build
build:
	@echo "Building app image..."
	docker build \
		--build-arg BUILD_TIME=$(shell date -u +'%Y-%m-%dT%H:%M:%SZ') \
		--build-arg GIT_COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo 'unknown') \
		-t $(APP_IMAGE):$(TAG) \
		./app
	@echo "Built: $(APP_IMAGE):$(TAG)"

.PHONY: build-bot
build-bot:
	@echo "Building remediation bot image..."
	docker build -t $(BOT_IMAGE):$(TAG) ./remediation
	@echo "Built: $(BOT_IMAGE):$(TAG)"

.PHONY: push
push: build
	docker push $(APP_IMAGE):$(TAG)
	@echo "Pushed: $(APP_IMAGE):$(TAG)"

.PHONY: push-bot
push-bot: build-bot
	docker push $(BOT_IMAGE):$(TAG)
	@echo "Pushed: $(BOT_IMAGE):$(TAG)"

.PHONY: build-minikube
build-minikube:
	@echo "Building image inside minikube..."
	eval $$(minikube docker-env) && \
		docker build -t $(APP_IMAGE):$(TAG) ./app && \
		docker build -t $(UI_IMAGE):$(TAG) ./ui && \
		docker build -t $(BOT_IMAGE):$(TAG) ./remediation
	@echo "Images built in minikube's Docker daemon"

.PHONY: deploy-app
deploy-app:
	@echo "Deploying app + UI + postgres..."
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/postgres.yaml
	kubectl apply -f k8s/deployment.yaml
	kubectl apply -f k8s/service.yaml
	kubectl apply -f k8s/ui-deployment.yaml
	kubectl apply -f k8s/ui-service.yaml
	kubectl apply -f k8s/hpa.yaml
	kubectl apply -f k8s/pdb.yaml
	kubectl apply -f k8s/ingress.yaml
	kubectl apply -f k8s/servicemonitor.yaml
	@echo "Waiting for app to be ready..."
	kubectl rollout status deployment/demo-app -n app --timeout=120s
	kubectl rollout status deployment/demo-ui -n app --timeout=120s
	@echo "App deployed successfully!"
	kubectl get pods -n app

.PHONY: deploy-bot
deploy-bot:
	@echo "Deploying remediation bot..."
	kubectl apply -f k8s/remediation-bot.yaml
	kubectl rollout status deployment/remediation-bot -n monitoring --timeout=60s
	@echo "Remediation bot deployed successfully!"

.PHONY: deploy-monitoring
deploy-monitoring:
	@echo "Applying alert rules and service monitors..."
	kubectl apply -f monitoring/alert-rules.yaml
	kubectl apply -f monitoring/grafana-dashboard.yaml
	@echo "Alert rules and Grafana dashboard applied successfully!"

# ---------------------------------------------------------------------------
# PREDICTIVE PLATFORM TARGETS
# ---------------------------------------------------------------------------

.PHONY: build-streaming
build-streaming:
	@echo "Building streaming images inside minikube..."
	eval $$(minikube docker-env) && \
		docker build -t $(PRODUCER_IMAGE):$(TAG) ./streaming/producer && \
		docker build -t $(CONSUMER_IMAGE):$(TAG) ./streaming/consumer
	@echo "Streaming images built in minikube's Docker daemon"

.PHONY: build-ml-minikube
build-ml-minikube:
	@echo "Building ML training image inside minikube..."
	eval $$(minikube docker-env) && \
		docker build -t $(ML_IMAGE):$(TAG) ./ml
	@echo "ML image built"

.PHONY: deploy-streaming
deploy-streaming: build-streaming
	@echo "Deploying metrics producer + predictor consumer..."
	kubectl apply -f k8s/predictor-consumer.yaml
	kubectl rollout status deployment/metrics-producer -n monitoring --timeout=60s
	@echo "Waiting for consumer (model load may take a moment)..."
	kubectl rollout status deployment/predictor-consumer -n monitoring --timeout=90s
	@echo "Streaming services deployed!"

.PHONY: train-model
train-model:
	@echo "Generating synthetic training data + training LSTM..."
	@echo "MLflow UI available at http://localhost:5000 after: make port-mlflow"
	cd ml && \
		pip install --quiet torch==2.3.1+cpu \
			--index-url https://download.pytorch.org/whl/cpu 2>/dev/null; \
		pip install --quiet -r requirements.txt 2>/dev/null; \
		python data_generator.py --synthetic --output training_data.csv && \
		python train.py \
			--data training_data.csv \
			--mlflow-uri http://localhost:5000 \
			--epochs 30 \
			--promote-threshold 0.75
	@echo "Training complete! Check MLflow: make port-mlflow"

.PHONY: train-model-synthetic
train-model-synthetic:
	@echo "Training LSTM on synthetic data (no Prometheus required)..."
	cd ml && \
		python data_generator.py --synthetic --output training_data.csv && \
		python train.py \
			--data training_data.csv \
			--mlflow-uri http://localhost:5000 \
			--epochs 30 \
			--promote-threshold 0.75

.PHONY: check-drift
check-drift:
	@echo "Running drift detection..."
	@[ -f ml/training_data.csv ] || (echo "ERROR: ml/training_data.csv not found. Run 'make train-model' first." && exit 1)
	cd ml && python -c "\
	import pandas as pd; \
	df = pd.read_csv('training_data.csv'); \
	n = len(df); \
	df.iloc[:int(n*0.8)].to_csv('reference_data.csv', index=False); \
	df.iloc[int(n*0.8):].to_csv('current_data.csv', index=False)"
	cd ml && python drift_check.py \
		--reference ml/reference_data.csv \
		--current ml/current_data.csv \
		--mlflow-uri http://localhost:5000 \
		--output ml/drift_report.json
	cat ml/drift_report.json

.PHONY: simulate-predictive
simulate-predictive:
	@echo "=== Predictive Demo ==="
	@echo "1. Simulating sustained CPU load (90s)..."
	@echo "   Watch consumer logs: make watch-predictions"
	@echo "   Watch predictions:   curl http://localhost:9000/predictions"
	@echo ""
	kubectl run predictive-load --rm -it --restart=Never \
		--image=curlimages/curl:latest \
		-- sh -c '\
		  for i in 1 2 3; do \
		    echo "Load wave $$i:"; \
		    curl -s -X POST "http://demo-app-service.app.svc.cluster.local/load?seconds=90"; \
		    sleep 10; \
		  done'

.PHONY: watch-predictions
watch-predictions:
	kubectl logs -f deploy/predictor-consumer -n monitoring

.PHONY: port-mlflow
port-mlflow:
	@echo "MLflow UI running at http://localhost:5000"
	@echo "Login: no auth (in-cluster SQLite mode)"
	kubectl port-forward svc/mlflow 5000:5000 -n monitoring

.PHONY: logs-producer
logs-producer:
	kubectl logs -f deploy/metrics-producer -n monitoring

.PHONY: logs-consumer
logs-consumer:
	kubectl logs -f deploy/predictor-consumer -n monitoring



.PHONY: simulate-crash
simulate-crash:
	@echo "Simulating crash (hitting /crash endpoint)..."
	@echo "Watch: make watch-pods"
	@echo ""
	$(eval APP_URL := $(shell kubectl get svc demo-app-service -n app -o jsonpath='{.spec.clusterIP}'):80)
	kubectl run crash-trigger --rm -it --restart=Never \
		--image=curlimages/curl:latest \
		-- sh -c 'for i in 1 2 3 4 5 6; do echo "Crash $$i:"; curl -s http://demo-app-service.app.svc.cluster.local/crash || true; sleep 2; done'

.PHONY: simulate-load
simulate-load:
	@echo "Simulating CPU load (60 seconds)..."
	kubectl run load-trigger --rm -it --restart=Never \
		--image=curlimages/curl:latest \
		-- curl -X POST "http://demo-app-service.app.svc.cluster.local/load?seconds=60"

.PHONY: watch-pods
watch-pods:
	kubectl get pods -n app -o wide -w

.PHONY: watch-hpa
watch-hpa:
	kubectl get hpa -n app -w

.PHONY: port-app
port-app:
	@echo "App backend running at http://localhost:8000"
	kubectl port-forward svc/demo-app-service 8000:80 -n app

.PHONY: port-ui
port-ui:
	@echo "UI running at http://localhost:3000"
	kubectl port-forward svc/demo-ui-service 3000:80 -n app

.PHONY: port-grafana
port-grafana:
	@echo "Grafana running at http://localhost:3000"
	@echo "Login: admin / grafana-admin-pass"
	kubectl port-forward svc/kube-prometheus-stack-grafana 3000:80 -n monitoring

.PHONY: port-prometheus
port-prometheus:
	@echo "Prometheus running at http://localhost:9090"
	kubectl port-forward svc/kube-prometheus-stack-prometheus 9090:9090 -n monitoring

.PHONY: port-alertmanager
port-alertmanager:
	@echo "Alertmanager running at http://localhost:9093"
	kubectl port-forward svc/kube-prometheus-stack-alertmanager 9093:9093 -n monitoring

.PHONY: port-bot
port-bot:
	@echo "Remediation bot running at http://localhost:9000"
	kubectl port-forward svc/remediation-bot-service 9000:9000 -n monitoring

.PHONY: port-mlflow
port-mlflow:
	@echo "MLflow UI available at http://localhost:5000"
	kubectl port-forward svc/mlflow 5000:5000 -n monitoring

.PHONY: logs-producer
logs-producer:
	kubectl logs -f deploy/metrics-producer -n monitoring

.PHONY: logs-consumer
logs-consumer:
	kubectl logs -f deploy/predictor-consumer -n monitoring


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------

.PHONY: logs-app
logs-app:
	kubectl logs -f deploy/demo-app -n app --all-containers

.PHONY: logs-bot
logs-bot:
	kubectl logs -f deploy/remediation-bot -n monitoring

# ---------------------------------------------------------------------------
# Terraform
# ---------------------------------------------------------------------------

TF_DIR := terraform/environments/dev

.PHONY: tf-init
tf-init:
	cd $(TF_DIR) && terraform init -backend=false

.PHONY: tf-validate
tf-validate:
	cd $(TF_DIR) && terraform fmt -recursive ../.. && terraform validate
	@echo "Terraform validation passed."

.PHONY: tf-plan
tf-plan:
	cd $(TF_DIR) && terraform plan -var="db_password=demo-password-not-real"

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

.PHONY: clean
clean:
	@echo "Removing 'app' namespace..."
	kubectl delete namespace app --ignore-not-found
	@echo "Done."

.PHONY: clean-monitoring
clean-monitoring:
	helm uninstall kube-prometheus-stack -n monitoring || true
	helm uninstall grafana -n monitoring || true

.PHONY: clean-predictive
clean-predictive:
	kubectl delete -f k8s/predictor-consumer.yaml --ignore-not-found
	kubectl delete -f k8s/kafka.yaml --ignore-not-found
	kubectl delete -f k8s/mlflow.yaml --ignore-not-found
	@echo "Predictive components removed."

.PHONY: minikube-stop
minikube-stop:
	minikube stop

.PHONY: minikube-delete
minikube-delete:
	minikube delete
	@echo "minikube cluster deleted."
