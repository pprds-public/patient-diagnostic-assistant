#!/bin/bash

# WIP

function usage() {
  echo "Usage: docker-build.sh orchestrator|normalizer|diagnoser|symptoms-extractor|planner"
  echo "  [-h|--help]"
  echo
  echo "Arguments:"
  echo "  orchestrator|normalizer|diagnoser|symptoms-extractor|planner      The service to build"
  echo "  -h|--help                                                         Display this help text"
}

run_docker_build=false
service_to_build=*

while [[ $# -gt 0 ]]; do
  key="$1"

  case $key in
  -h | --help)
    usage
    exit 0
    ;;
  *)
    echo "ERROR: unknown argument $1"
    echo
    usage
    exit 1
    ;;
  esac
done

function docker_build() {
  #docker build -f ./orchestrator/Dockerfile -t pprd-systems/patient-diagnostic-assistant/orchestrator:1.0.0 .
  #docker build -f ./normalizer/Dockerfile -t pprd-systems/patient-diagnostic-assistant/normalizer:1.0.0 .
  #docker build -f ./diagnoser/Dockerfile -t pprd-systems/patient-diagnostic-assistant/diagnoser:1.0.0 .
  #docker build -f ./symptos-extractor/Dockerfile -t pprd-systems/patient-diagnostic-assistant/symptos-extractor:1.0.0 .
  #docker build -f ./planner/Dockerfile -t pprd-systems/patient-diagnostic-assistant/planner:1.0.0 .
}

docker_build_cmd="docker_build_cmd"

if [ "run_docker_build" = true ]; then
  docker_build_cmd="docker_build"
fi

docker_build_cmd