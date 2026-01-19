#!/bin/bash

set -e

NS=mediacorr

run_job () {
  JOB=$1
  echo "▶ Ejecutando $JOB"
  kubectl apply -f ../02-jobs/$JOB.yaml -n $NS
  kubectl wait --for=condition=complete job/$JOB -n $NS
}

run_job sources-job
run_job ingestor-job
run_job filter-job
run_job classifier-job
run_job correlator-job

echo "✅ Pipeline completado"
