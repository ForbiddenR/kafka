FROM golang:1.24.2-alpine3.21 AS base

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 go build -o kafka ./

FROM alpine:3.21

WORKDIR /app

COPY --from=base /app/kafka kafka
COPY config.yaml config.yaml

CMD ["./kafka", "consume", "-t", "back_pressure_test"]
