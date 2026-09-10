#!/bin/sh
#
# Starts a three-master Valkey cluster inside a single container.
#
# All nodes share the container's loopback interface, so cluster gossip works
# over 127.0.0.1, while the host reaches the same addresses through published
# ports. `--cluster-announce-*` makes redirects point at 127.0.0.1:<port>
# rather than the container's bridge IP, which a host cannot route on Docker
# Desktop.
set -eu

PORTS="7000 7001 7002"
DATA_ROOT=/data

for port in $PORTS; do
  mkdir -p "$DATA_ROOT/$port"
done

for port in $PORTS; do
  valkey-server \
    --port "$port" \
    --dir "$DATA_ROOT/$port" \
    --cluster-enabled yes \
    --cluster-config-file nodes.conf \
    --cluster-node-timeout 5000 \
    --cluster-announce-ip 127.0.0.1 \
    --cluster-announce-port "$port" \
    --cluster-announce-bus-port "$((port + 10000))" \
    --appendonly no \
    --save "" &
done

for port in $PORTS; do
  until valkey-cli -p "$port" ping >/dev/null 2>&1; do
    sleep 0.2
  done
done

# Form the cluster once; a restarted container keeps its nodes.conf.
if ! valkey-cli -p 7000 cluster info | grep -q 'cluster_state:ok'; then
  valkey-cli --cluster create \
    127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
    --cluster-replicas 0 --cluster-yes
fi

wait
