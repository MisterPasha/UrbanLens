# Both targets use the AWS Python runtime, including its local runtime interface emulator.
ARG LAMBDA_BASE_IMAGE=public.ecr.aws/lambda/python@sha256:d0acb0bce6c7b14a193a911f800bee8ff73fd28bc8d01e953db2f5aea32d270e
FROM ${LAMBDA_BASE_IMAGE} AS base
RUN --mount=type=secret,id=build_ca \
    if [ -f /run/secrets/build_ca ]; then export PIP_CERT=/run/secrets/build_ca; fi; \
    pip install --no-cache-dir uv==0.13.6
WORKDIR /var/task
COPY pyproject.toml uv.lock ./

FROM base AS api
RUN --mount=type=secret,id=build_ca \
    if [ -f /run/secrets/build_ca ]; then export SSL_CERT_FILE=/run/secrets/build_ca; fi; \
    uv export --locked --no-dev --no-emit-project --format requirements-txt > /tmp/requirements.txt \
    && uv pip install --system --python /var/lang/bin/python --require-hashes -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
COPY src/urbanlens ./urbanlens
CMD ["urbanlens.api.lambda_entry.handler"]

FROM base AS pipeline
RUN --mount=type=secret,id=build_ca \
    if [ -f /run/secrets/build_ca ]; then \
      cp /etc/pki/tls/certs/ca-bundle.crt /tmp/original-ca.crt; \
      cat /run/secrets/build_ca >> /etc/pki/tls/certs/ca-bundle.crt; \
    fi; \
    dnf install -y postgresql17 && dnf clean all \
    && if [ -f /tmp/original-ca.crt ]; then \
      cat /tmp/original-ca.crt > /etc/pki/tls/certs/ca-bundle.crt; rm /tmp/original-ca.crt; \
    fi
RUN --mount=type=secret,id=build_ca \
    if [ -f /run/secrets/build_ca ]; then export SSL_CERT_FILE=/run/secrets/build_ca; fi; \
    uv export --locked --no-dev --extra pipeline --no-emit-project --format requirements-txt > /tmp/requirements.txt \
    && uv pip install --system --python /var/lang/bin/python --require-hashes -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
COPY src/urbanlens ./urbanlens
COPY scripts ./scripts
COPY config ./config
COPY dbt ./dbt
COPY reference ./reference
ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false HOME=/tmp PYTHONPYCACHEPREFIX=/tmp/pycache
CMD ["urbanlens.operations.assets.collector_handler"]
