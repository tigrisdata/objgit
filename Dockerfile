# syntax=docker/dockerfile:1

# --- build stage -------------------------------------------------------------
# Pin the build stage to the platform of the runner ($BUILDPLATFORM) and
# cross-compile through TARGETOS and TARGETARCH. One native runner then
# builds every target platform, and no emulation is necessary. A plain
# `docker build` sets these values to the host platform, which keeps the
# previous behavior.
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS
ARG TARGETARCH

WORKDIR /src

# Cache module downloads separately from the source tree.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# internal/kustomize/kustomize.wasm is a Git LFS object. A checkout without
# LFS has the pointer file there, and the build would embed it. Stop here
# when the file does not start with the WebAssembly magic.
RUN head -c 4 internal/kustomize/kustomize.wasm | od -An -tx1 | grep -q '00 61 73 6d' || \
    { echo "internal/kustomize/kustomize.wasm is not WebAssembly; run git lfs pull" >&2; exit 1; }

# Static, stripped binary. No cgo: objgitd is pure Go and answers the git
# protocol natively (no `git` binary at runtime).
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" \
    -o /objgitd ./cmd/objgitd

# --- runtime stage -----------------------------------------------------------
# distroless static: just CA certs (for Tigris/S3 TLS) + tzdata, nonroot user.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /objgitd /objgitd

# Smart HTTP, git://, metrics. SSH (-ssh-bind) is opt-in; publish it yourself.
EXPOSE 8080 9418 9090

ENTRYPOINT ["/objgitd"]
