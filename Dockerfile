FROM golang:1.22 AS builder
WORKDIR /src
COPY go.mod go.sum* ./
COPY . .
RUN go build -o /out/proks .

FROM gcr.io/distroless/base-debian12
COPY --from=builder /out/proks /proks
EXPOSE 8080
ENTRYPOINT ["/proks"]
