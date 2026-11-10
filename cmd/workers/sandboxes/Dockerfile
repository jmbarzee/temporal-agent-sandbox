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
    go build -o /bin/worker ./cmd/workers/sandboxes

FROM alpine:3.23
COPY --from=buildstage /bin/worker /bin/worker
ENTRYPOINT ["/bin/worker"]
