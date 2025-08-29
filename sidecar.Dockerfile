FROM golang:1.24.4-bookworm AS build

RUN apt-get update && apt-get install -y gcc libc6-dev && rm -rf /var/lib/apt/lists/*

RUN mkdir /app
WORKDIR /app

RUN mkdir jitsubase sync-sidecar

COPY jitsubase/go.* ./jitsubase/
COPY bulkerlib/go.* ./bulkerlib/
COPY sync-sidecar/go.* ./sync-sidecar/

RUN go work init jitsubase bulkerlib sync-sidecar

WORKDIR /app/sync-sidecar

RUN go mod download

WORKDIR /app

COPY . .

# Build bulker
RUN go build -o sidecar ./sync-sidecar

#######################################
# FINAL STAGE
FROM debian:bookworm-slim AS final
ENV DEBIAN_FRONTEND=noninteractive

# Base runtime packages and tools for converting/installing Oracle RPMs
RUN apt-get update && apt-get install -y --no-install-recommends \
    alien \
    rpm \
    ca-certificates \
    curl \
    libaio1 \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install Oracle Instant Client (detect architecture inside the container)
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
    amd64) oracle_url="https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm" ;; \
    arm64) oracle_url="https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-linux-arm64.rpm" ;; \
    *) echo "Unsupported architecture: $arch"; exit 1 ;; \
    esac; \
    wget -O oracle-instantclient-basic.rpm "$oracle_url"; \
    alien -i --scripts oracle-instantclient-basic.rpm; \
    rm -f oracle-instantclient*.rpm

ENV TZ=UTC

RUN mkdir /app
WORKDIR /app

# Copy bulkerapp
COPY --from=build /app/sidecar ./

CMD ["/app/sidecar"]
