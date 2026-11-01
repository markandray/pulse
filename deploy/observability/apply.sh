#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

kubectl apply -f prometheus.yaml
kubectl apply -f jaeger.yaml
kubectl create configmap grafana-dashboards \
  --from-file=pulse.json=dashboards/pulse.json \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f grafana.yaml
kubectl rollout status deployment/jaeger --timeout=180s
kubectl rollout status deployment/prometheus deployment/grafana --timeout=180s