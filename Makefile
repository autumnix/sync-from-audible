IMAGE_NAME=autumnix/sync-from-audible
LOCAL_IMAGE=sync-from-audible:test
VERSION?=v0.1.0
PLATFORMS=linux/amd64,linux/arm64
BUILDER?=multiarch-builder

docker-build:
	docker build -t $(LOCAL_IMAGE) .

docker-run:
	docker run --rm $(LOCAL_IMAGE)

buildx-create:
	@if ! docker buildx inspect $(BUILDER) >/dev/null 2>&1; then \
		docker buildx create --use --name $(BUILDER); \
	else \
		docker buildx use $(BUILDER); \
	fi
	docker buildx inspect --bootstrap

docker-release: buildx-create
	docker buildx build \
		--platform $(PLATFORMS) \
		-t $(IMAGE_NAME):latest \
		-t $(IMAGE_NAME):$(VERSION) \
		--push .

release: docker-release
