#!/usr/bin/env bash
#
# Put the environment back to the state the course's demos start from.
#
# Run it whenever you want to replay a clip from the beginning. It starts the VM if you
# deallocated it, then:
#
#   1. deletes the run history of the course's own Dags, named one by one below
#   2. recreates the Connections from .env
#
# Later modules add steps here as their demos need them.
#
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$HERE/.env"
[ -f "$ENV_FILE" ] || { echo "ERROR: no $ENV_FILE - run scripts/1_provision_azure.sh first" >&2; exit 1; }

get() { grep -E "^$1=" "$ENV_FILE" | cut -d= -f2-; }
SUB=$(get AZ_SUBSCRIPTION); RG=$(get AZ_RESOURCE_GROUP); VM_NAME=$(get VM_NAME)
VM_IP=$(get VM_PUBLIC_IP); VM_USER=$(get VM_USER); KEYFILE=$(get VM_SSH_KEY)

SSH="ssh -i $KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# The Dags whose history a replay should not see. Named explicitly rather than matched by
# pattern, so a reset never touches a Dag it wasn't written for.
COURSE_DAGS="'globoticket_request_summary', 'globoticket_request_extract'"

# --- 0. is the VM running? ----------------------------------------------------
say "Checking the VM"
STATE=$(az vm get-instance-view -g "$RG" -n "$VM_NAME" --subscription "$SUB" \
  --query "instanceView.statuses[?starts_with(code,'PowerState')].displayStatus|[0]" -o tsv)
if [ "$STATE" != "VM running" ]; then
  echo "  '$STATE' - starting it"
  az vm start -g "$RG" -n "$VM_NAME" --subscription "$SUB" -o none
  # A restarted VM can come back with a new public IP. Read it back rather than trust .env.
  NEW_IP=$(az vm show -d -g "$RG" -n "$VM_NAME" --subscription "$SUB" --query publicIps -o tsv)
  if [ "$NEW_IP" != "$VM_IP" ]; then
    echo "  the public IP changed ($VM_IP -> $NEW_IP); re-run scripts/1_provision_azure.sh to update .env" >&2
    exit 1
  fi
  echo "  started; waiting for Airflow"
  for i in $(seq 1 40); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 6 "http://$VM_IP:8080/api/v2/monitor/health" || true)" = 200 ] && break
    sleep 8
  done
else
  echo "  running"
fi

# --- 1. run history -----------------------------------------------------------
say "Dag run history"
$SSH "$VM_USER@$VM_IP" "cd /opt/globoticket && sudo docker compose exec -T postgres psql -U airflow -d airflow -q -t -c \
  \"DELETE FROM dag_run WHERE dag_id IN ($COURSE_DAGS) RETURNING dag_id;\"" | grep -c . | sed 's/^/  runs deleted: /' || true

# --- 2. connections -----------------------------------------------------------
say "Connections"
"$HERE/scripts/3_create_connections.sh"
