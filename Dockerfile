FROM ghcr.io/astral-sh/uv:0.12.12 AS uv

FROM public.ecr.aws/dataminded/spark-k8s-glue:v4.0.1-hadoop-3.4.2-v4

USER 0
ENV PYSPARK_PYTHON python3
WORKDIR /opt/spark/work-dir

COPY --from=uv /uv /uvx /usr/local/bin/

COPY pyproject.toml uv.lock README.md ./
COPY src ./src

RUN uv sync --locked

ENV PATH="/opt/spark/work-dir/.venv/bin:$PATH"

ENTRYPOINT ["python3", "-m", "capstonellm.tasks.clean"]
