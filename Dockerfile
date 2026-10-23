# syntax=docker/dockerfile:1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e

# Building on the native platform and cross-compiling avoids QEMU emulation.
FROM --platform=$BUILDPLATFORM golang:1.27.3@sha256:f20d83a83e18dbd2a4714b8ffd4b30b287b6243962dd75608e86abcc63ba4efc AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

# VERSION reaches main.version, which kong reports through --version.
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
# The build cache is keyed per target so the two platform legs do not evict
# each other; the module cache is shared because it is platform independent.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build,id=go-build-$TARGETOS-$TARGETARCH \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /tranquila .

FROM gcr.io/distroless/static-debian13@sha256:15f8637509792e05b97d83451b0b6ccfa4abd548f997e406ac1e72c87fe1fa78
COPY --from=builder /tranquila /tranquila

ENTRYPOINT ["/tranquila", "sync"]
