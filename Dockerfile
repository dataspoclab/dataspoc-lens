FROM python:3.12-slim

LABEL maintainer="DataSpoc <dev@dataspoc.com>"
LABEL description="DataSpoc Lens — Virtual warehouse. SQL over cloud Parquet via DuckDB."

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install --no-cache-dir uv

# Copy project
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src/ src/

# Install dataspoc-lens with all extras (s3, gcs, azure, jupyter, ai)
RUN uv pip install --system -e ".[all]"

# Initialize config structure
RUN dataspoc-lens init

# Local lake directory for dev/testing
RUN mkdir -p /lake

# Jupyter port
EXPOSE 8888

# Volumes
VOLUME ["/root/.dataspoc-lens", "/lake"]

ENTRYPOINT ["dataspoc-lens"]
CMD ["--help"]
