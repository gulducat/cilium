#!/usr/bin/env bash

if [ ! "$(uname -s)" == 'Linux' ]; then
  echo 'must run on linux...'
  exit 1
fi

# https://hub.docker.com/r/cilium/cilium/tags
TAG='v1.14.4'

# pull cilium binaries from their docker image
# mainly what we *need* is cilium-cni, but
# cilium cli is helpul too.

CP=$(cat <<SCRIPT
cp /opt/cni/bin/cilium-cni /host/cni/bin/
cp /usr/bin/cilium* /host/bin/
cp /usr/bin/hubble /host/bin/
SCRIPT
)

sudo docker run --rm -it \
	-v /opt/cni:/host/cni \
	-v /usr/local/bin:/host/bin \
	"cilium/cilium:$TAG" \
	bash -xc "$CP"

# if you want to run cilium agent directly on linux machine,
# need `bpftool` (and other stuff?):
exit # remove this line to do the extra stuff
sudo apt update
sudo apt install \
	linux-tools-common \
	linux-tools-$(uname --kernel-release) \
  ;
