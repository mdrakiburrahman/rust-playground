#!/bin/bash
#
#
#       Bootstraps a Linux Devbox host for the VS Code devcontainer idempotently.
#       If your Devbox restarts, rerun this script.
#
# ---------------------------------------------------------------------------------------
#

REPO_ROOT=$(git rev-parse --show-toplevel)
DOCKER_VERSION="5:27.5.1-1~ubuntu.24.04~noble"

reset_docker_state() {
  docker ps -aq | xargs -r docker rm -f &&
    docker volume prune -af &&
    docker network prune -f
}

# Remove Windows paths from PATH to avoid using Windows az CLI
# This allows us to mount ~/.azure from WSL.
#
export PATH=$(echo "$PATH" | tr ':' '\n' | grep -v "/mnt/c" | tr '\n' ':' | sed 's/:$//')
AZ_PATH=$(which az 2>/dev/null)
if [[ -z "$AZ_PATH" || "$AZ_PATH" == *"/mnt/c"* ]]; then
  echo "Native Linux Azure CLI not found, installing..."
  curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
  export PATH="$HOME/bin:$PATH"
  [[ -f "$HOME/.bashrc" ]] && source "$HOME/.bashrc"
else
  echo "Native Linux Azure CLI already installed at: $AZ_PATH"
fi

for pkg in jq wslu; do
  if ! dpkg -s "$pkg" >/dev/null 2>&1; then
    echo "$pkg is not installed on your devbox, installing..."
    sudo apt-get update >/dev/null && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y "$pkg" >/dev/null
  fi
done

if ! command -v gh >/dev/null 2>&1; then
  echo "GitHub CLI is not installed on your devbox, installing..."
  (type -p wget >/dev/null || (sudo apt update && sudo apt install wget -y)) \
    && sudo mkdir -p -m 755 /etc/apt/keyrings \
    && out=$(mktemp) && wget -nv -O"$out" https://cli.github.com/packages/githubcli-archive-keyring.gpg \
    && cat "$out" | sudo tee /etc/apt/keyrings/githubcli-archive-keyring.gpg >/dev/null \
    && rm -f "$out" \
    && sudo chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
    && sudo mkdir -p -m 755 /etc/apt/sources.list.d \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list >/dev/null \
    && sudo apt update \
    && sudo apt install gh -y
else
  echo "GitHub CLI is already installed."
fi

az account get-access-token --query "expiresOn" -o tsv >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    echo "az is not logged in, logging in..."
    az login >/dev/null
fi

if ! [ -x "$(command -v docker)" ]; then
  echo "docker is not installed on your devbox, installing..."
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
  sudo add-apt-repository -y "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
  sudo apt-get update -q
  sudo apt-get install -y apt-transport-https ca-certificates curl
  sudo apt-get install -y --allow-downgrades docker-ce="$DOCKER_VERSION" docker-ce-cli="$DOCKER_VERSION" containerd.io
fi

[[ -f /proc/sys/user/max_user_namespaces ]] && [[ "$(cat /proc/sys/user/max_user_namespaces)" -eq 0 ]] && sudo sysctl -w user.max_user_namespaces=28633
[[ -f /proc/sys/kernel/unprivileged_userns_clone ]] && [[ "$(cat /proc/sys/kernel/unprivileged_userns_clone)" -ne 1 ]] && sudo sysctl -w kernel.unprivileged_userns_clone=1

sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json > /dev/null <<'EOF'
{
  "max-concurrent-downloads": 32,
  "max-concurrent-uploads": 32,
  "default-ulimits": {
    "nofile": { "Name": "nofile", "Hard": 1048576, "Soft": 1048576 },
    "nproc":  { "Name": "nproc",  "Hard": 1048576, "Soft": 1048576 },
    "memlock": { "Name": "memlock", "Hard": -1, "Soft": -1 }
  },
  "features": { "buildkit": true },
  "log-driver": "json-file",
  "log-opts": { "max-size": "50m", "max-file": "3" }
}
EOF

echo "docker is installed, restarting..."
sudo systemctl restart docker

sudo chmod 666 /var/run/docker.sock
reset_docker_state || exit 1

if ! [ -x "$(command -v npm)" ]; then
  echo "npm is not installed on your devbox, installing..."
  curl -fsSL https://deb.nodesource.com/setup_lts.x | sudo -E bash -
  sudo apt-get update 2>&1 > /dev/null
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y nodejs
else
  echo "npm is already installed."
fi

sudo npm install -g @devcontainers/cli

echo "Docker: $(docker --version)"
echo "npm: $(npm version)"
echo "Dev Containers CLI: $(devcontainer --version)"