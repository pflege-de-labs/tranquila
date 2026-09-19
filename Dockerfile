# syntax=docker/dockerfile:1@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

# Building on the native platform and cross-compiling avoids QEMU emulation.
FROM --platform=$BUILDPLATFORM golang:1.27.1@sha256:3680233e3204827fbdc66088528ae6d4b3d034f51d03a99d454f6de034888244 AS builder
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

FROM gcr.io/distroless/static-debian13@sha256:58133991db06659feaabe0f4e97a35cebf15ef4ea08f8a4c6d2ee5f75e4aa6a0
COPY --from=builder /tranquila /tranquila

ENTRYPOINT ["/tranquila", "sync"]
