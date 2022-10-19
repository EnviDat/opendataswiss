ARG EXTERNAL_REG
ARG INTERNAL_REG
ARG PYTHON_IMG_TAG
FROM ${INTERNAL_REG}/debian:bullseye as certs



FROM ${EXTERNAL_REG}/python:${PYTHON_IMG_TAG}-slim-bullseye as base

ARG APP_VERSION
ARG PYTHON_IMG_TAG
ARG MAINTAINER
LABEL envidat.ch.app-version="${APP_VERSION}" \
      envidat.ch.python-img-tag="${PYTHON_IMG_TAG}" \
      envidat.ch.maintainer="${MAINTAINER}"

# CA-Certs
COPY --from=certs \
    /etc/ssl/certs/ca-certificates.crt \
    /etc/ssl/certs/ca-certificates.crt
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

RUN set -ex \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install \
       -y --no-install-recommends locales \
    && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

# Set locale
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && locale-gen
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8



FROM base as build
RUN set -ex \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install \
        -y --no-install-recommends \
            build-essential \
            gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/python
COPY pyproject.toml pdm.lock /opt/python/
COPY __version__.py /opt/python/
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir pdm==2.0.2 \
    && pdm config python.use_venv false
RUN pdm install --prod --no-editable



FROM base as runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONFAULTHANDLER=1

RUN set -ex \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install \
        -y --no-install-recommends \
            curl \
    && rm -rf /var/lib/apt/lists/*

ARG PYTHON_IMG_TAG
COPY --from=build \
    "/opt/python/__pypackages__/${PYTHON_IMG_TAG}/lib" \
    /opt/python/pkgs
ENV PYTHONPATH="/opt/python/pkgs"
WORKDIR /opt/app
COPY main.py ./

# Upgrade pip & pre-compile deps to .pyc, add appuser, permissions
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -c "import compileall; compileall.compile_path(maxlevels=10, quiet=1)" \
    && useradd -r -u 900 -m -c "unprivileged account" -d /home/appuser -s /bin/false appuser \
    && chown -R appuser:appuser /opt



FROM runtime as debug
WORKDIR /opt/python
COPY pyproject.toml pdm.lock ./
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN pip install --no-cache-dir pdm==2.0.2 \
    && pdm config python.use_venv false \
    && pdm export --dev --no-default | \
    pip install --no-cache-dir -r /dev/stdin
WORKDIR /opt/app
USER appuser
ENTRYPOINT ["python", "-m", "debugpy", "--wait-for-client", "--listen", "0.0.0.0:5678"]
CMD ["/opt/app/main.py"]



FROM runtime as prod
USER appuser
ENTRYPOINT ["python"]
CMD ["/opt/app/main.py"]
