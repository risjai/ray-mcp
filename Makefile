.PHONY: build test lint vet tidy envtest-crds test-envtest e2e-up test-e2e e2e-down e2e pre-push

# Version pins (KubeRay + envtest K8s) are sourced from a single file so tier 2
# (envtest) and tier 5 (kind/helm e2e) stay behaviorally aligned.
include hack/kuberay-version.env
export KUBERAY_VERSION ENVTEST_K8S_VERSION

# Where the fetched KubeRay CRD bundle lands (gitignored). The envtest smoke
# test points CRDDirectoryPaths at this dir.
CRD_DIR := test/crds
CRD_BASE_URL := https://raw.githubusercontent.com/ray-project/kuberay/$(KUBERAY_VERSION)/ray-operator/config/crd/bases
KIND_CLUSTER := ray-mcp-e2e

# Build all packages.
build:
	go build ./...

# Run the unit + MCP tiers (tiers 1, 3, 4). No build tags, no envtest binaries,
# no Docker — this is the fast per-save loop and must stay green everywhere.
test:
	go test ./...

# Run the linter via the version pinned as a module tool in go.mod.
lint:
	go tool golangci-lint run ./...

# Run go vet.
vet:
	go vet ./...

# Sync module requirements.
tidy:
	go mod tidy

# Fetch the 3 KubeRay CRD yamls at $(KUBERAY_VERSION) into $(CRD_DIR) for the
# envtest tier. Idempotent: skips any CRD already present.
envtest-crds:
	@mkdir -p $(CRD_DIR)
	@for crd in rayclusters rayjobs rayservices; do \
		dst="$(CRD_DIR)/ray.io_$$crd.yaml"; \
		if [ -f "$$dst" ]; then \
			echo "envtest-crds: $$dst present, skipping"; \
		else \
			echo "envtest-crds: fetching ray.io_$$crd.yaml @ $(KUBERAY_VERSION)"; \
			curl -fsSL "$(CRD_BASE_URL)/ray.io_$$crd.yaml" -o "$$dst"; \
		fi; \
	done

# Tier 2: envtest (kube-apiserver + etcd + KubeRay CRDs, NO operator, NO Docker).
# Fetches the CRD bundle, resolves the setup-envtest assets for the pinned K8s
# version, and runs the -tags envtest suite against them.
test-envtest: envtest-crds
	KUBEBUILDER_ASSETS="$$(go tool setup-envtest use -p path $(ENVTEST_K8S_VERSION))" \
		go test -tags envtest ./...

# Tier 5 (e2e): create a kind cluster and install the KubeRay operator via Helm,
# both pinned to $(KUBERAY_VERSION), then wait for the operator to be available.
# Requires Docker + kind.
e2e-up:
	kind create cluster --name $(KIND_CLUSTER) --config test/e2e/kind-config.yaml
	helm repo add kuberay https://ray-project.github.io/kuberay-helm/
	helm repo update
	helm install kuberay-operator kuberay/kuberay-operator \
		--version $(patsubst v%,%,$(KUBERAY_VERSION)) \
		--namespace kuberay-system --create-namespace --wait
	kubectl rollout status deployment/kuberay-operator -n kuberay-system --timeout=180s

# Tier 5 (e2e): run the -tags e2e suite against the running kind cluster (reads
# the kind kubeconfig from the current KUBECONFIG / default context).
test-e2e:
	go test -tags e2e ./...

# Tier 5 (e2e): tear down the kind cluster.
e2e-down:
	kind delete cluster --name $(KIND_CLUSTER)

# Tier 5 (e2e): full cycle — up, test, down (down runs even if the tests fail).
e2e:
	$(MAKE) e2e-up
	$(MAKE) test-e2e; status=$$?; $(MAKE) e2e-down; exit $$status

# Pre-push gate: run all runnable tiers (unit/MCP + envtest + e2e) before pushing
# a cluster-touching task. Soft discipline, not a hard hook (testing-strategy §3).
pre-push: test test-envtest e2e
