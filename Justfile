module := "github.com/ethandedalus/single-decree-paxos"
tailwind_version := "4.3.3"

default: generate build

tools:
    go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
    go install github.com/a-h/templ/cmd/templ@latest

generate: proto templ tailwind

proto:
    protoc -I proto \
        --go_out=. --go_opt=module={{ module }} \
        --go-grpc_out=. --go-grpc_opt=module={{ module }} \
        $(find proto -name '*.proto')

templ:
    templ generate

tailwind: _tailwindcss
    ./bin/tailwindcss -i web/app.css -o pkg/ui/static/app.css --minify

_tailwindcss:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -x bin/tailwindcss ]; then
        exit 0
    fi
    case "$(uname -s)" in
        Darwin) platform=macos ;;
        Linux)  platform=linux ;;
        *)      echo "unsupported os: $(uname -s)" >&2; exit 1 ;;
    esac
    case "$(uname -m)" in
        arm64|aarch64) platform="${platform}-arm64" ;;
        x86_64|amd64)  platform="${platform}-x64" ;;
        *)             echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
    esac
    url="https://github.com/tailwindlabs/tailwindcss/releases/download/v{{ tailwind_version }}/tailwindcss-${platform}"
    echo "downloading ${url}"
    mkdir -p bin
    curl -sSfL "$url" -o bin/tailwindcss
    chmod +x bin/tailwindcss

build:
    go build ./...

test *args:
    go test ./... {{ args }}

race:
    go test -race ./...

scenarios:
    go test -race -count=1 -v ./pkg/paxostest/

vet:
    go vet ./...

fmt:
    gofmt -w .

tidy:
    go mod tidy

check: fmt vet race

up *args:
    docker compose up -d --build {{ args }}

down:
    docker compose down -v

logs *args:
    docker compose logs -f {{ args }}

image:
    docker build --build-arg CMD=node -t paxos-node:latest .
    docker build --build-arg CMD=console -t paxos-console:latest .
