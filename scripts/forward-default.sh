#!/bin/bash

function kill_subprocess() {
  echo ""
  for proc in $(jobs -p); do
    echo "killing background process ${proc}"
    kill ${proc}
  done
}
trap kill_subprocess SIGINT
trap kill_subprocess EXIT

namespace=cnch
server_name='default-server'
pod_name=$(kubectl --namespace=${namespace} get pod | grep $server_name | awk 'NR==1{print $1}')
echo "forwarding ${pod_name}"
(
  set -x
  kubectl port-forward --namespace=${namespace} pod/${pod_name} 9010
)
