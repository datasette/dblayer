# The dblayer benchmark machine: toolchains only. The repository itself is
# cloned at run time, at the git ref given to fly/launch.sh, and every
# backend is built from that fresh clone (fly/machine/run.sh).
#
#   Debian 13 (trixie), amd64
#   PostgreSQL 18 from apt.postgresql.org (server in the same machine)
#   uv, with CPython 3.12, 3.13 and 3.14
#   Node.js 24 (newest 24.x at build time)
#   Rust stable (minimal profile, plus clippy)
#   gcc/g++ (build-essential), make, curl, unzip, git
#
# Build and push with fly/build-image.sh. No secrets are built in.
FROM debian:trixie-slim

ARG UV_VERSION=0.13.0
ARG NODE_MAJOR=24
ARG PG_MAJOR=18

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    RUSTUP_HOME=/opt/rust/rustup \
    CARGO_HOME=/opt/rust/cargo \
    UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    PATH=/opt/rust/cargo/bin:/opt/node/bin:/usr/lib/postgresql/${PG_MAJOR}/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg git build-essential pkg-config unzip xz-utils \
        procps psmisc util-linux time lsof less jq \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt trixie-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-${PG_MAJOR} postgresql-client-${PG_MAJOR} \
    && rm -rf /var/lib/apt/lists/*

# Node.js: the official tarball of the newest release of the major version,
# checked against the release's SHASUMS256.txt.
RUN set -eux; \
    base="https://nodejs.org/dist/latest-v${NODE_MAJOR}.x"; \
    name="$(curl -fsSL "$base/SHASUMS256.txt" | awk '/linux-x64.tar.xz$/ {print $2}')"; \
    curl -fsSL -o "/tmp/$name" "$base/$name"; \
    curl -fsSL "$base/SHASUMS256.txt" | grep " $name\$" | (cd /tmp && sha256sum -c -); \
    mkdir -p /opt/node; tar -xJf "/tmp/$name" -C /opt/node --strip-components=1; rm "/tmp/$name"; \
    node --version; npm --version

# Rust stable with clippy, readable by every user.
RUN set -eux; \
    curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable -c clippy --no-modify-path; \
    chmod -R a+rwX /opt/rust; rustc --version; cargo clippy --version

# uv and the three Python versions the project supports.
RUN set -eux; \
    curl -fsSL "https://github.com/astral-sh/uv/releases/download/${UV_VERSION}/uv-x86_64-unknown-linux-gnu.tar.gz" \
        | tar -xz -C /usr/local/bin --strip-components=1; \
    uv --version; \
    uv python install 3.12 3.13 3.14; \
    chmod -R a+rX /opt/uv-python

# The unprivileged user the benchmarks and PostgreSQL run as (some tests
# check file permissions, which root ignores).
RUN useradd --create-home --uid 1000 --shell /bin/bash bench

COPY machine/ /fly/machine/
RUN chmod +x /fly/machine/*.sh

CMD ["/fly/machine/entry.sh"]
