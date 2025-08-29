FROM golang:1.24.4-bookworm AS build

RUN apt-get install -y gcc libc6-dev

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
ARG TARGETPLATFORM

RUN apt-get update -y && apt-get install -y alien ca-certificates curl libaio1 wget && rm -rf /var/lib/apt/lists/*

RUN echo "$TARGETPLATFORM"

# Install Oracle Instant Client
RUN if [ "$TARGETPLATFORM" = "linux/amd64" ]; then \
    wget -O oracle-instantclient-basic.rpm https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm; \
    elif [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
    wget -O oracle-instantclient-basic.rpm https://download.oracle.com/otn_software/linux/instantclient/instantclient-basic-linux-arm64.rpm; \
    else \
    echo "Unsupported platform: $TARGETPLATFORM"; \
    exit 1; \
    fi


RUN alien -i --scripts oracle-instantclient-basic.rpm

RUN rm -f oracle-instantclient*.rpm

ENV TZ=UTC

RUN mkdir /app
WORKDIR /app

# Copy bulkerapp
COPY --from=build /app/sidecar ./

CMD ["/app/sidecar"]
