FROM golang:1.24.4-bookworm AS build

RUN apt-get install gcc libc6-dev

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

RUN apt-get update -y
RUN apt-get install -y ca-certificates curl libaio1 alien wget

RUN wget https://yum.oracle.com/repo/OracleLinux/OL8/oracle/instantclient23/x86_64/getPackage/oracle-instantclient-basic-23.4.0.24.05-1.el8.x86_64.rpm

RUN alien -i --scripts oracle-instantclient-basic-23.4.0.24.05-1.el8.x86_64.rpm

# RUN wget https://yum.oracle.com/repo/OracleLinux/OL8/oracle/instantclient23/x86_64/getPackage/oracle-instantclient-sqlplus-23.4.0.24.05-1.el8.x86_64.rpm

# RUN alien -i --scripts oracle-instantclient-sqlplus-23.4.0.24.05-1.el8.x86_64.rpm

RUN rm -f oracle-instantclient*.rpm


ENV TZ=UTC

RUN mkdir /app
WORKDIR /app

# Copy bulkerapp
COPY --from=build /app/sidecar ./

CMD ["/app/sidecar"]
