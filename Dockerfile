# Stage 1: Build the kube-doctor Go binary (supports native & cross-compilation for amd64/arm64/arm)
FROM --platform=$BUILDPLATFORM golang:alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH
ARG TARGETVARIANT

WORKDIR /workspace

COPY go.mod ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/

RUN CGO_ENABLED=0 \
    GOOS=${TARGETOS:-linux} \
    GOARCH=${TARGETARCH:-$(go env GOARCH)} \
    GOARM=${TARGETVARIANT#v} \
    go build -ldflags="-s -w" -o /out/kube-doctor ./cmd/kube-doctor

# Stage 2: Minimal runtime image (no RUN steps needed; certs copied from builder to avoid QEMU/seccomp SIGSYS 159)
FROM alpine:3.20

WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /out/kube-doctor /app/kube-doctor
COPY knowledge/ /app/knowledge/
COPY configs/ /app/configs/

USER 10001:10001
EXPOSE 8080

ENV PORT="8080"
ENV KNOWLEDGE_BASE_PATH="/app/knowledge/k8s_1_35_ocp_4_22.json"

ENTRYPOINT ["/app/kube-doctor"]

