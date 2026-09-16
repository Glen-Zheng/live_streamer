FROM ubuntu:24.04

RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libcurl4-openssl-dev \
    libxml2-dev \
    libssl-dev \
    ca-certificates \
    libdrm2 \
    libxcb1 \
    libxcb-shm0 \
    libxau6 \
    libxdmcp6 \
    libbsd0 \
    libmd0 \
    zlib1g \
    zlib1g-dev \
    libbz2-1.0 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

CMD ["/bin/bash"]
