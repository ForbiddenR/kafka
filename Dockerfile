FROM golang:1.24.3-alpine3.21 AS base

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 go build -o kafka ./

FROM alpine:3.21

WORKDIR /app

COPY --from=base /app/kafka kafka
COPY config.yaml config.yaml
COPY command.line command.line
COPY protocol.line protocol.line

CMD ["./kafka", "statistics", "test"]
