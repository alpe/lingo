#!/bin/bash
set -xeuo pipefail

source $REPO_DIR/test/e2e/common.sh

model="opt-125m-cpu"

function cleanup() {
  echo "++++++++++++++"
  kubectl delete configmap k6 || true
  kubectl delete -f $TEST_DIR/k6-pod.yaml || true
  kubectl delete -f $TEST_DIR/model.yaml || true
  kubectl delete pods -l app.kubernetes.io/name=kubeai || true
}

trap cleanup EXIT
#kubectl label ns gpu-operator pod-security.kubernetes.io/enforce=privileged
#kubectl label nodes --all run.ai/simulated-gpu-node-pool=default

# Run a constant-user load generation pod.
# This should trigger the autoscaler to scale up the model.
kubectl create configmap k6 --from-file $TEST_DIR/k6.js || true
kubectl create -f $TEST_DIR/k6-pod.yaml

kubectl apply -f $TEST_DIR/model.yaml

kubectl wait --timeout=3m --for=condition=Ready pod/k6

kubectl wait --timeout=3m --for=create pod/opt-125m-cpu-0
kubectl wait --timeout=4m --for=condition=Ready pod/opt-125m-cpu-0
kubectl wait --timeout=60s --for=jsonpath='{.spec.replicas}'=2 model/$model

# Stop load generation pod.
kubectl delete --now -f $TEST_DIR/k6-pod.yaml || true

# Restart KubeAI without load.
kubectl delete pods -l app.kubernetes.io/name=kubeai || true
echo "pod deletion done"
# Model should be scaled down.
kubectl wait --timeout=60s --for=jsonpath='{.spec.replicas}'=0 model/$model
