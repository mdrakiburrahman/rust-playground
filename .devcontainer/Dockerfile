# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/devcontainers/base:ubuntu-24.04

ARG DEBIAN_FRONTEND=noninteractive

RUN printf '%s\n' \
        'Acquire::Retries "2";' \
        'Acquire::http::Timeout "30";' \
        'Acquire::https::Timeout "30";' \
        'APT::Update::Error-Mode "any";' \
        > /etc/apt/apt.conf.d/99-rust-playground-transport \
    && find /etc/apt -type f \( -name '*.list' -o -name '*.sources' \) \
        -exec sed -i -E 's#http://(archive|security)\.ubuntu\.com/ubuntu([/[:space:]]|$)#https://azure.archive.ubuntu.com/ubuntu\2#g' {} + \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        curl \
        git \
        jq \
        unzip \
        libssl-dev \
        lldb \
        mold \
        pkg-config \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

ARG KUBECTL_VERSION=v1.35.6
ARG KUBECTL_SHA256=5d11e2ba01ea68ffd053f56e27738e2b4330013ee67f7e46c6da6c585d3c9926
ARG K9S_VERSION=v0.51.0
ARG K9S_SHA256=c3752ad51a5a4015a113819c4eeb6e55a4d0e4b8e652494797532f6fc8161dd7
ARG KUBELOGIN_VERSION=v0.2.22
ARG KUBELOGIN_SHA256=716cbcb1880bca8f0e976b8917d06fcfe04de2c1e737eaaabbb26e2782330119
ARG KUSTOMIZE_VERSION=v5.8.3
ARG KUSTOMIZE_SHA256=cb9e31198d3f63b44848bd0afc4c8efc56e6cfef9c93a4a39a211940e6decd61

RUN set -eu; \
    test \
      "$(dpkg --print-architecture)" = amd64; \
    curl --fail --location --silent --show-error \
      "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/amd64/kubectl" \
      --output /tmp/kubectl; \
    echo "$KUBECTL_SHA256  /tmp/kubectl" | sha256sum --check --status; \
    curl --fail --location --silent --show-error \
      "https://github.com/derailed/k9s/releases/download/$K9S_VERSION/k9s_Linux_amd64.tar.gz" \
      --output /tmp/k9s.tar.gz; \
    echo "$K9S_SHA256  /tmp/k9s.tar.gz" | sha256sum --check --status; \
    curl --fail --location --silent --show-error \
      "https://github.com/Azure/kubelogin/releases/download/$KUBELOGIN_VERSION/kubelogin-linux-amd64.zip" \
      --output /tmp/kubelogin.zip; \
    echo "$KUBELOGIN_SHA256  /tmp/kubelogin.zip" | sha256sum --check --status; \
    curl --fail --location --silent --show-error \
      "https://github.com/kubernetes-sigs/kustomize/releases/download/kustomize/$KUSTOMIZE_VERSION/kustomize_${KUSTOMIZE_VERSION}_linux_amd64.tar.gz" \
      --output /tmp/kustomize.tar.gz; \
    echo "$KUSTOMIZE_SHA256  /tmp/kustomize.tar.gz" | sha256sum --check --status; \
    install -m 0755 /tmp/kubectl /usr/local/bin/kubectl; \
    tar -xzOf /tmp/k9s.tar.gz k9s > /usr/local/bin/k9s; \
    chmod 0755 /usr/local/bin/k9s; \
    unzip -p /tmp/kubelogin.zip bin/linux_amd64/kubelogin > /usr/local/bin/kubelogin; \
    chmod 0755 /usr/local/bin/kubelogin; \
    tar -xzOf /tmp/kustomize.tar.gz kustomize > /usr/local/bin/kustomize; \
    chmod 0755 /usr/local/bin/kustomize; \
    rm -f /tmp/kubectl /tmp/k9s.tar.gz /tmp/kubelogin.zip /tmp/kustomize.tar.gz
