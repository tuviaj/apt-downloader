FROM ubuntu:24.04
SHELL ["/bin/bash", "-eo", "pipefail", "-c"]

# system deps (python + apt tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl gnupg apt-transport-https python3 \
    && rm -rf /var/lib/apt/lists/*

# install uv (static binary)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /app

# copy only project files first (for better caching)
COPY pyproject.toml ./

# sync dependencies with uv (installs confluent-kafka etc.)
RUN uv sync --frozen

# now copy source
COPY src ./src

# ensure output dir exists
RUN mkdir -p /out

# default run command (can override)
CMD ["uv", "run", "apt-bundler", "kafka"]
