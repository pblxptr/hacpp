FROM ubuntu:24.04

# Avoid prompts during installation
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3 \
    python3-requests \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /home/env
