#!/usr/bin/env bash

set -ex

WORKDIR="$1"

if [ -z "$WORKDIR" ]
then
    echo "WORKDIR variable must be defined."
    exit 1
fi

echo
date "+build_indexer begin DEPLOY stage %Y%m%d_%H%M%S"
echo

FULLVERSION=${VERSION:-$("$WORKDIR/scripts/compute_build_number.sh")}
PKG_DIR="$WORKDIR/packages/$FULLVERSION"
SIGNING_KEY_ADDR=dev@algorand.com

chmod 400 "$HOME/.gnupg"

apt-get install aptly -y

cat <<EOF>"$HOME/.aptly.conf"
{
  "rootDir": "$HOME/aptly",
  "downloadConcurrency": 4,
  "downloadSpeedLimit": 0,
  "architectures": [],
  "dependencyFollowSuggests": false,
  "dependencyFollowRecommends": false,
  "dependencyFollowAllVariants": false,
  "dependencyFollowSource": false,
  "dependencyVerboseResolve": false,
  "gpgDisableSign": false,
  "gpgDisableVerify": false,
  "gpgProvider": "gpg",
  "downloadSourcePackages": false,
  "skipLegacyPool": true,
  "ppaDistributorID": "ubuntu",
  "ppaCodename": "",
  "skipContentsPublishing": false,
  "FileSystemPublishEndpoints": {},
  "S3PublishEndpoints": {
    "algorand-releases": {
      "region":"us-east-1",
      "bucket":"algorand-releases",
      "acl":"public-read",
      "prefix":"indexer/deb"
    }
  },
  "SwiftPublishEndpoints": {}
}
EOF

DEB="$PKG_DIR/algorand-indexer_${FULLVERSION}_${ARCH}.deb"
DIST=stable
SNAPSHOT="${DIST}-${FULLVERSION}"
aptly repo create -distribution="$DIST" -component=main algorand-indexer
aptly repo add algorand-indexer "$DEB"
aptly snapshot create "$SNAPSHOT" from repo algorand-indexer
aptly publish snapshot -gpg-key="$SIGNING_KEY_ADDR" -origin=Algorand -label=Algorand "$SNAPSHOT" "s3:algorand-releases:"

echo
date "+build_indexer end DEPLOY stage %Y%m%d_%H%M%S"
echo

