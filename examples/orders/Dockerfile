FROM golang:1.25-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/orders ./examples/orders

FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=builder /out/orders /usr/local/bin/orders
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/orders"]
