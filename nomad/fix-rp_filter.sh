#!/bin/bash

echo 'overriding some rp_filter per https://github.com/cilium/cilium/issues/10645'

set -x

echo 'net.ipv4.conf.lxc*.rp_filter = 0' \
	| sudo tee /etc/sysctl.d/90-cilium.conf \
	&& sudo systemctl start systemd-sysctl

