# syntax=docker/dockerfile:1

# Build natively and cross-compile: no QEMU per target architecture.
FROM --platform=$BUILDPLATFORM golang:1.27.3-alpine AS buildstage
WORKDIR /src
ARG TARGETOS TARGETARCH
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    sh -c 'go build -o /bin/worker ./cmd/workers/sidecar && go build -o /bin/execd ./cmd/execd'

FROM alpine:3.23
# The chart's init container copies execd into the sandbox container.
COPY --from=buildstage /bin/worker /bin/execd /bin/
ENTRYPOINT ["/bin/worker"]
