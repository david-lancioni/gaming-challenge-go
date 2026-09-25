# syntax=docker/dockerfile:1
# Go version must match go.mod (go 1.26.2).
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/wagering ./cmd/wagering

FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget && adduser -D -u 10001 wagering
COPY --from=build /out/wagering /usr/local/bin/wagering
USER wagering
EXPOSE 8080 9100
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=10 \
    CMD wget -qO- http://127.0.0.1:8080/health/live >/dev/null || exit 1
ENTRYPOINT ["wagering"]
CMD ["serve"]
