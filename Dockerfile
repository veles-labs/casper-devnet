FROM ubuntu:22.04 AS builder
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    git \
    libssl-dev \
    pkg-config \
  && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --profile minimal --default-toolchain stable

WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY src ./src

RUN cargo build --release --locked

ENV XDG_DATA_HOME=/opt/casper-devnet-data
RUN mkdir -p "$XDG_DATA_HOME" \
    && /app/target/release/casper-devnet assets pull

FROM ubuntu:22.04
ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgcc-s1 \
    libssl3 \
    libstdc++6 \
  && rm -rf /var/lib/apt/lists/*

ENV XDG_DATA_HOME=/opt/casper-devnet-data

COPY --from=builder /app/target/release/casper-devnet /usr/local/bin/casper-devnet
COPY --from=builder /opt/casper-devnet-data /opt/casper-devnet-data

VOLUME ["/opt/casper-devnet-data"]

EXPOSE 11101 14101 18101 22101 28101

CMD ["casper-devnet", "start"]
