FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /guard ./cmd/guard

# Minimal, non-root runtime image: no shell, no package manager.
FROM gcr.io/distroless/static:nonroot
COPY --from=build /guard /guard
ENV DLQ_PATH=/tmp/dead-letter.jsonl
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/guard"]
