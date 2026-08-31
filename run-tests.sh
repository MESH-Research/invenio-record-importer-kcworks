#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2025 MESH Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.

# Quit on errors
set -o errexit

# Quit on unbound symbols
set -o nounset

# Always bring down docker services
function cleanup() {
  eval "$(uv run docker-services-cli down --env)"
}

# Resolve docker-services-cli compose YAML (same package ``up`` uses).
function docker_services_cli_yml_path() {
  local resolved candidate
  if resolved="$(
    uv run python -c \
      "from pathlib import Path; import docker_services_cli; \
print(Path(docker_services_cli.__file__).parent / 'docker-services.yml')" \
      2>/dev/null
  )" && [ -n "${resolved}" ] && [ -f "${resolved}" ]; then
    echo "${resolved}"
    return 0
  fi
  # Fallback when uv/import fails or Python minor version differs from a
  # hardcoded --filepath in older scripts.
  for candidate in .venv/lib/python*/site-packages/docker_services_cli/docker-services.yml; do
    if [ -f "${candidate}" ]; then
      echo "${candidate}"
      return 0
    fi
  done
  return 1
}

# Host ports for services this runner will start (from compose YAML, not hardcoded).
function docker_services_cli_expected_host_ports() {
  local yml ports_str script_dir helper
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  helper="${script_dir}/scripts/docker_services_cli_host_ports.py"
  # Match the service kinds passed to ``docker-services-cli up`` below.
  local services="${DB:-postgresql},${CACHE:-redis},${SEARCH:-opensearch},${MQ:-rabbitmq}"

  if ! yml="$(docker_services_cli_yml_path 2>/dev/null)"; then
    echo "Warning: could not locate docker-services-cli compose file; using fallback ports." >&2
    echo "5432 6379 9200 9300 5672 15672"
    return 0
  fi
  if [ ! -f "$helper" ]; then
    echo "Warning: missing ${helper}; using fallback ports." >&2
    echo "5432 6379 9200 9300 5672 15672"
    return 0
  fi
  if ! ports_str="$(uv run python "$helper" "$yml" "$services" 2>/dev/null)"; then
    echo "Warning: failed to parse host ports from ${yml}; using fallback ports." >&2
    echo "5432 6379 9200 9300 5672 15672"
    return 0
  fi
  if [ -z "${ports_str// /}" ]; then
    echo "Warning: no host ports found for services (${services}); using fallback ports." >&2
    echo "5432 6379 9200 9300 5672 15672"
    return 0
  fi
  echo "$ports_str"
}

# Check for containers that would collide with docker-services-cli host ports.
# Name/image matches alone are not enough: local stacks (e.g. kcworks-next) often
# publish the same services on different host ports and can coexist.
function check_docker_compose_running() {
  echo "Checking for containers that conflict with docker-services-cli ports..."

  local expected_ports
  # shellcheck disable=SC2207
  expected_ports=($(docker_services_cli_expected_host_ports))
  echo "Expected docker-services-cli host ports: ${expected_ports[*]}"

  local candidates
  candidates=$(
    docker ps --format '{{.Names}}\t{{.Image}}\t{{.Ports}}' \
      | grep -E '(postgres|redis|opensearch|rabbitmq|elasticsearch)' || true
  )

  if [ -z "$candidates" ]; then
    echo "No related service containers detected."
    return 0
  fi

  local conflicts=""
  local ok_related=""

  while IFS=$'\t' read -r name image ports; do
    [ -z "${name:-}" ] && continue

    # Already the docker-services-cli project — reuse, do not treat as conflict.
    if [[ "$name" == docker_services_cli-* ]]; then
      ok_related+="  ${name} (docker-services-cli; OK to reuse)"$'\n'
      continue
    fi

    local hit_ports=()
    local p
    for p in "${expected_ports[@]}"; do
      # Host publish form is host:HOSTPORT->containerport/...
      if echo "$ports" | grep -Eq ":${p}->"; then
        hit_ports+=("$p")
      fi
    done

    if [ ${#hit_ports[@]} -gt 0 ]; then
      conflicts+="  ${name}	${image}	host ports: ${hit_ports[*]}"$'\n'
    else
      ok_related+="  ${name} (related name/image, different host ports; OK)"$'\n'
    fi
  done <<< "$candidates"

  if [ -n "$ok_related" ]; then
    echo "Related containers without docker-services-cli port conflicts:"
    printf "%s" "$ok_related"
  fi

  if [ -n "$conflicts" ]; then
    echo "Warning: Found containers publishing ports docker-services-cli needs:"
    printf "%s" "$conflicts"
    echo ""
    echo "This will cause port conflicts with docker-services-cli."
    echo "Consider stopping those containers before continuing."
    echo ""
    read -p "Do you want to continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
      echo "Aborting. Please stop conflicting containers and try again."
      exit 1
    fi
  else
    echo "No port conflicts with docker-services-cli detected."
  fi
}

# Check for arguments
# Note: "-k" would clash with "pytest"
keep_services=0
pytest_args=()
skip_translations=0
for arg in $@; do
  # from the CLI args, filter out some known values and forward the rest to "pytest"
  # note: we don't use "getopts" here b/c of some limitations (e.g. long options),
  #       which means that we can't combine short options (e.g. "./run-tests -Kk pattern")
  case ${arg} in
  -K | --keep-services)
    keep_services=1
    ;;
  -S | --skip-translations)
    skip_translations=1
    ;;
  *)
    pytest_args+=(${arg})
    ;;
  esac
done

if [[ ${keep_services} -eq 0 ]]; then
  trap cleanup EXIT
fi

# Check for running docker-compose projects before starting services
check_docker_compose_running

# Check if tests/.env exists and set env_file_arg accordingly
if [ -f "tests/.env" ]; then
  env_file_arg="--env-file tests/.env"
  echo "Using tests/.env file for environment variables"
else
  env_file_arg=""
  echo "No tests/.env file found, using default environment"
fi

# Start the services and get their environment variables
echo "Starting the services"
eval "$(uv run ${env_file_arg} docker-services-cli --filepath .venv/lib/python3.12/site-packages/docker_services_cli/docker-services.yml up --db ${DB:-postgresql} --cache ${CACHE:-redis} --search opensearch --mq ${MQ:-rabbitmq} --env)"

# Unset the environment variables that docker-services-cli set so that the values from tests/.env are used instead of those defaults from docker-services.yml
unset SQLALCHEMY_DATABASE_URI
unset INVENIO_SQLALCHEMY_DATABASE_URI

# Run mypy
echo "Running mypy on invenio_record_importer_kcworks"
uv run mypy --config-file pyproject.toml invenio_record_importer_kcworks

# Note: expansion of pytest_args looks like below to not cause an unbound
# variable error when 1) "nounset" and 2) the array is empty.
if [ ${#pytest_args[@]} -eq 0 ]; then
  echo "Running pytest"
  uv run ${env_file_arg} python -m pytest -vv -s --disable-warnings --cov=invenio_record_importer_kcworks --cov-report=term-missing
else
  echo "Running pytest with additional arguments"
  uv run ${env_file_arg} python -m pytest ${pytest_args[@]} -s --disable-warnings --cov=invenio_record_importer_kcworks --cov-report=term-missing
fi

tests_exit_code=$?
exit "$tests_exit_code"
