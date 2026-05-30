# syntax=docker/dockerfile:1

# --- build stage -------------------------------------------------------------
FROM golang:1.26 AS build

WORKDIR /src

# Cache module downloads separately from the source tree.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Static, stripped binary. No cgo: objgitd is pure Go and answers the git
# protocol natively (no `git` binary at runtime).
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" \
    -o /objgitd ./cmd/objgitd

# --- runtime stage -----------------------------------------------------------
# distroless static: just CA certs (for Tigris/S3 TLS) + tzdata, nonroot user.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /objgitd /objgitd

# Smart HTTP, git://, metrics. SSH (-ssh-bind) is opt-in; publish it yourself.
EXPOSE 8080 9418 9090

ENTRYPOINT ["/objgitd"]
