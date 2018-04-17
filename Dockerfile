FROM python:3.6.5-slim-stretch

LABEL maintainer="miller@adammiller.io"

RUN pip3 install prometheus_client==0.2.0 requests

ADD couchbase-exporter.py /opt/couchbase-exporter/couchbase-exporter.py

RUN useradd couchbase-exporter \
        --home /opt/couchbase-exporter \
        -K UID_MIN=10000 \
        -K GID_MIN=10000 && \
    chmod +x /opt/couchbase-exporter/couchbase-exporter.py && \
    chown couchbase-exporter /opt/couchbase-exporter

WORKDIR /opt/couchbase-exporter

USER couchbase-exporter

EXPOSE 8080

ENTRYPOINT [ "python3", "/opt/couchbase-exporter/couchbase-exporter.py" ] 