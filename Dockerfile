FROM golang:1.26.2-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG CMD=node

RUN CGO_ENABLED=0 GOOS=linux go build \
	-trimpath \
	-ldflags="-s -w" \
	-o /out/app ./cmd/${CMD}

FROM scratch

COPY --from=build /out/app /app

EXPOSE 8080 8090 9000

ENTRYPOINT ["/app"]
