#!/bin/sh
set -eu

. /etc/os-release
if [ "$ID" != debian ] || [ "$VERSION_CODENAME" != bookworm ] || [ "$(dpkg --print-architecture)" != amd64 ]; then
    echo "Azure CLI runtime installation requires Debian bookworm amd64." >&2
    exit 1
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends ca-certificates curl gpg

# Microsoft Learn's signed apt repository, with the signing key content pinned.
curl --fail --silent --show-error --location \
    https://packages.microsoft.com/keys/microsoft.asc \
    --output /tmp/arrowcol-azure-cli-microsoft.asc
printf '%s\n' '2fa9c05d591a1582a9aba276272478c262e95ad00acf60eaee1644d93941e3c6  /tmp/arrowcol-azure-cli-microsoft.asc' \
    | sha256sum --check --strict -
gpg --batch --yes --dearmor \
    --output /usr/share/keyrings/arrowcol-azure-cli.gpg \
    /tmp/arrowcol-azure-cli-microsoft.asc
printf '%s\n' 'deb [arch=amd64 signed-by=/usr/share/keyrings/arrowcol-azure-cli.gpg] https://packages.microsoft.com/repos/azure-cli/ bookworm main' \
    > /etc/apt/sources.list.d/arrowcol-azure-cli.list

apt-get update
apt-get install -y --no-install-recommends azure-cli=2.91.0-1~bookworm
test "$(dpkg-query -W -f='${Version}' azure-cli)" = '2.91.0-1~bookworm'

apt-get purge -y --auto-remove gpg
rm -f /tmp/arrowcol-azure-cli-microsoft.asc \
    /usr/share/keyrings/arrowcol-azure-cli.gpg \
    /etc/apt/sources.list.d/arrowcol-azure-cli.list
find /var/lib/apt/lists -type f -delete
