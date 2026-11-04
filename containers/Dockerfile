# Both targets use the AWS Python runtime, including its local runtime interface emulator.
ARG LAMBDA_BASE_IMAGE=public.ecr.aws/lambda/python:3.13
FROM ${LAMBDA_BASE_IMAGE} AS base
RUN pip install --no-cache-dir uv==0.13.6
WORKDIR /var/task
COPY pyproject.toml uv.lock ./

FROM base AS api
RUN uv export --locked --no-dev --no-emit-project --format requirements-txt > /tmp/requirements.txt \
    && uv pip install --system --python /var/lang/bin/python --require-hashes -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
COPY src/urbanlens ./urbanlens
CMD ["urbanlens.lambda_handlers.api_handler"]

FROM base AS pipeline
RUN dnf install -y postgresql17 && dnf clean all
RUN uv export --locked --no-dev --extra pipeline --no-emit-project --format requirements-txt > /tmp/requirements.txt \
    && uv pip install --system --python /var/lang/bin/python --require-hashes -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
COPY src/urbanlens ./urbanlens
ENV DBT_SEND_ANONYMOUS_USAGE_STATS=false HOME=/tmp PYTHONPYCACHEPREFIX=/tmp/pycache
CMD ["urbanlens.lambda_handlers.pipeline_handler"]
