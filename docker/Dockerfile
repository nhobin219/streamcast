FROM python:3.12-slim-bookworm AS build
COPY --from=ghcr.io/astral-sh/uv:0.13.9 /uv /usr/local/bin/uv
WORKDIR /opt/streamcast
COPY pyproject.toml uv.lock README.md LICENSE NOTICE ./
COPY src ./src
# Health probes share a five-second budget with imports. The read-only runtime
# cannot cache bytecode, so compile it before concurrent probes start.
RUN uv sync --frozen --no-dev --no-editable --compile-bytecode

FROM python:3.12-slim-bookworm AS runtime
LABEL org.opencontainers.image.source="https://github.com/nhobin219/streamcast" \
      org.opencontainers.image.licenses="Apache-2.0"
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 streamcast \
    && useradd --uid 10001 --gid 10001 --no-create-home streamcast \
    && install -d -o 10001 -g 10001 /var/lib/streamcast /run/streamcast \
    && install -d -m 0700 -o 10001 -g 10001 /run/streamcast-control
COPY --from=build /opt/streamcast/.venv /opt/streamcast/.venv
ENV PATH="/opt/streamcast/.venv/bin:$PATH" \
    HOME="/tmp" \
    PYTHONDONTWRITEBYTECODE="1"
USER 10001:10001
ENTRYPOINT ["tini", "--", "streamcast"]
CMD ["--help"]
