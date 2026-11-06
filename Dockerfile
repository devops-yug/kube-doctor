# Stage 1: Build the kube-doctor Go binary
FROM golang:1.23-alpine AS builder

WORKDIR /workspace

COPY go.mod ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /out/kube-doctor ./cmd/kube-doctor

# Stage 2: Minimal runtime image
FROM alpine:3.20

RUN apk --no-cache add ca-certificates && adduser -D -u 10001 kubedoctor

WORKDIR /app
COPY --from=builder /out/kube-doctor /app/kube-doctor
COPY knowledge/ /app/knowledge/
COPY configs/ /app/configs/

USER 10001
EXPOSE 8080

ENV PORT="8080"
ENV KNOWLEDGE_BASE_PATH="/app/knowledge/k8s_1_35_ocp_4_22.json"

ENTRYPOINT ["/app/kube-doctor"]

