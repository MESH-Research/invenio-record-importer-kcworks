#!/usr/bin/env bash
# -*- coding: utf-8 -*-
#
# Copyright (C) 2023-2026 MESH Research
#
# invenio-record-importer-kcworks is free software; you can redistribute it
# and/or modify it under the terms of the MIT License; see LICENSE file for
# more details.
#
# Package test runner. When this repo is nested under knowledge-commons-works
# (docker-compose.test.yml + docker/run-tests-wrapper.sh present), local runs
# exec the parent ``./run-tests.sh`` so pytest uses the same test-runner
# container isolation as the monorepo. Path args are rewritten relative to
# this package root; with no path args, defaults to this package's ``tests/``.
#
# Standalone clones (no parent harness) keep the host docker-services-cli +
# ``uv run pytest`` path below.

# Quit on errors
set -o errexit

# Quit on unbound symbols
set -o nounset

PACKAGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

function find_kcworks_root() {
  local d="$PACKAGE_DIR"
  while [ "$d" != "/" ]; do
    if [ -f "$d/docker-compose.test.yml" ] &&
      [ -f "$d/run-tests.sh" ] &&
      [ -f "$d/docker/run-tests-wrapper.sh" ]; then
      printf '%s\n' "$d"
      return 0
    fi
    d="$(dirname "$d")"
  done
  return 1
}

# Nested under KCWorks: use the parent test-runner (match root local mode).
if kcworks_root="$(find_kcworks_root)"; then
  package_rel="${PACKAGE_DIR#"${kcworks_root}"/}"
  if [[ "$package_rel" == "$PACKAGE_DIR" ]]; then
    echo "Error: could not relativize package path to KCWorks root ${kcworks_root}" >&2
    exit 1
  fi

  parent_args=()
  has_path_arg=0
  for arg in "$@"; do
    case ${arg} in
    -K | --keep-services | -S | --skip-translations | -B | --build | -J | --js | --js-only | --fetch-secrets)
      parent_args+=("${arg}")
      ;;
    -*)
      # Pytest flags (-k, -vv, --disable-warnings, …)
      parent_args+=("${arg}")
      ;;
    *)
      has_path_arg=1
      if [[ "${arg}" == /* ]]; then
        parent_args+=("${arg}")
      else
        parent_args+=("${package_rel}/${arg}")
      fi
      ;;
    esac
  done
  if [[ ${has_path_arg} -eq 0 ]]; then
    parent_args+=("${package_rel}/tests")
  fi

  echo "Nested under KCWorks at ${kcworks_root}; delegating to parent test-runner"
  echo "  ./run-tests.sh ${parent_args[*]}"
  cd "${kcworks_root}"
  exec ./run-tests.sh "${parent_args[@]}"
fi

# ── Standalone path (no parent test-runner) ───────────────────────────────

# Bring down docker-services-cli when we started them (not with -K).
function cleanup() {
  if [[ ${keep_services:-0} -eq 0 ]]; then
    # `--env` opens a `: '` shell comment, runs the command, then closes the
    # quote and prints the exports. A failed `down` never closes it, so only
    # eval output from a run that succeeded.
    local down_env
    if down_env="$(uv run docker-services-cli down --env)"; then
      eval "${down_env}"
    else
      echo "Warning: docker-services-cli down failed; service env left as is." >&2
    fi
  fi
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

function check_docker_compose_running() {
  echo "Checking for containers that conflict with docker-services-cli ports..."

  local expected_ports
  # shellcheck disable=SC2207
  expected_ports=($(docker_services_cli_expected_host_ports))
  echo "Expected docker-services-cli host ports: ${expected_ports[*]}"

  local candidates
  candidates=$(
    docker ps --format '{{.Names}}\t{{.Image}}\t{{.Ports}}' |
      grep -E '(postgres|redis|opensearch|rabbitmq|elasticsearch)' || true
  )

  if [ -z "$candidates" ]; then
    echo "No related service containers detected."
    return 0
  fi

  local conflicts=""
  local ok_related=""

  while IFS=$'\t' read -r name image ports; do
    [ -z "${name:-}" ] && continue

    if [[ "$name" == docker_services_cli-* ]]; then
      ok_related+="  ${name} (docker-services-cli; OK to reuse)"$'\n'
      continue
    fi

    local hit_ports=()
    local p
    for p in "${expected_ports[@]}"; do
      if echo "$ports" | grep -Eq ":${p}->"; then
        hit_ports+=("$p")
      fi
    done

    if [ ${#hit_ports[@]} -gt 0 ]; then
      conflicts+="  ${name}	${image}	host ports: ${hit_ports[*]}"$'\n'
    else
      ok_related+="  ${name} (related name/image, different host ports; OK)"$'\n'
    fi
  done <<<"$candidates"

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
skip_translations=0
pytest_args=()
for arg in "$@"; do
  case ${arg} in
  -K | --keep-services)
    keep_services=1
    ;;
  -S | --skip-translations)
    skip_translations=1
    ;;
  *)
    pytest_args+=("${arg}")
    ;;
  esac
done

if [[ ${keep_services} -eq 0 ]]; then
  trap cleanup EXIT
fi

translations_dir="invenio_record_importer_kcworks/assets/semantic-ui/translations"
if [ -d "${translations_dir}" ]; then
  if [[ ${skip_translations} -eq 0 ]]; then
    echo "Extracting translations from python files"
    uv run invenio-cli translations extract
    echo "Updating translations"
    uv run invenio-cli translations update
    echo "Compiling translations"
    uv run invenio-cli translations compile
  else
    echo "Skipping translations compilation"
  fi
else
  echo "No translations directory at ${translations_dir}; skipping translations step"
fi

if [ -d "docs/source/" ]; then
  echo "Building the documentation"
  uv run sphinx-build -b html docs/source/ docs/build/
else
  echo "No docs/source/ directory; skipping documentation build"
fi

check_docker_compose_running

env_file_args=()
if [ -f "tests/.env" ]; then
  env_file_args+=(--env-file tests/.env)
  echo "Using tests/.env file for non-secret environment variables"
else
  echo "No tests/.env file found"
fi

echo "Starting the services"
if ! services_yml="$(docker_services_cli_yml_path)"; then
  echo "Error: could not locate docker-services-cli compose file in the active environment." >&2
  exit 1
fi
echo "Using docker-services-cli compose file: ${services_yml}"
eval "$(uv run "${env_file_args[@]+"${env_file_args[@]}"}" docker-services-cli --filepath "${services_yml}" up --db ${DB:-postgresql} --cache ${CACHE:-redis} --search opensearch --mq ${MQ:-rabbitmq} --env)"

if ! uv pip show invenio-record-importer-kcworks 2>/dev/null | grep -q "Editable project location"; then
  echo "Installing package in editable mode"
  uv pip install -e .
else
  echo "Package already installed as editable, skipping installation"
fi

# ty: root run-tests.sh only runs it in CI (local test-runner does not).
if [ -n "${CI:-}" ]; then
  echo "Running ty on invenio_record_importer_kcworks (CI)"
  uv run ty check invenio_record_importer_kcworks
else
  echo "Skipping ty (local mode; set CI=1 to run type checks before pytest)"
fi

set +e
if [ ${#pytest_args[@]} -eq 0 ]; then
  echo "Running pytest"
  uv run "${env_file_args[@]+"${env_file_args[@]}"}" python -m pytest -vv -s --disable-warnings \
    --cov=invenio_record_importer_kcworks --cov-report=term-missing
else
  echo "Running pytest with additional arguments"
  uv run "${env_file_args[@]+"${env_file_args[@]}"}" python -m pytest "${pytest_args[@]}" -s --disable-warnings \
    --cov=invenio_record_importer_kcworks --cov-report=term-missing
fi
tests_exit_code=$?
set -e

exit "$tests_exit_code"
