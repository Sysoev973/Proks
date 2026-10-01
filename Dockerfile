FROM golang:1.25 AS builder
WORKDIR /src
COPY go.mod go.sum* ./
COPY . .
RUN go build -o /out/proks .
RUN go build -o /out/healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/base-debian12
COPY --from=builder /out/proks /proks
COPY --from=builder /out/healthcheck /healthcheck
EXPOSE 8080
ENTRYPOINT ["/proks"]
