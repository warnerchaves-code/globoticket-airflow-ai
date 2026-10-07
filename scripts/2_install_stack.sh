#!/usr/bin/env bash
#
# Step 2 of 3 - install Docker on the VM, ship the repo up, build the image, start the stack.
#
# Run this from your machine, from the repo root, after 1_provision_azure.sh.
# It is re-runnable: it re-syncs the files, rebuilds the image if anything changed, and
# restarts the stack.
#
# What ends up on the VM, in /opt/globoticket:
#   Dockerfile, sitecustomize.py   the Globoticket image: Airflow 3.3.2 + the Common AI Provider
#   docker-compose.yaml            Airflow (scheduler, api server, worker, triggerer, dag
#                                  processor, metadata PostgreSQL, Redis) and the
#                                  globoticket-pg event catalog database
#   dags/  assets/                 the course Dags and the sample promoter request
#   .env                           the generated secrets, copied from the repo root
#
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$HERE/.env"
[ -f "$ENV_FILE" ] || { echo "ERROR: no $ENV_FILE - run scripts/1_provision_azure.sh first" >&2; exit 1; }

get() { grep -E "^$1=" "$ENV_FILE" | cut -d= -f2-; }
VM_IP=$(get VM_PUBLIC_IP); VM_USER=$(get VM_USER); KEYFILE=$(get VM_SSH_KEY)
say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

SSH="ssh -i $KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SCP="scp -i $KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"

# --- wait for ssh -------------------------------------------------------------
say "Waiting for SSH on $VM_IP"
for i in $(seq 1 30); do
  if $SSH -o ConnectTimeout=8 "$VM_USER@$VM_IP" true 2>/dev/null; then echo "  up"; break; fi
  echo "  ...still waiting"
  [ "$i" = "30" ] && { echo "SSH never came up. Is the VM running, and is your public IP still $(get ALLOWED_IP)?" >&2; exit 1; }
done

# --- docker -------------------------------------------------------------------
say "Docker Engine + Compose plugin"
$SSH "$VM_USER@$VM_IP" bash -s <<'REMOTE'
set -euo pipefail
if command -v docker >/dev/null && docker compose version >/dev/null 2>&1; then
  echo "  already installed: $(docker --version)"
else
  export DEBIAN_FRONTEND=noninteractive
  sudo apt-get update -qq
  sudo apt-get install -y -qq ca-certificates curl gnupg >/dev/null
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  sudo apt-get update -qq
  sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin >/dev/null
  sudo usermod -aG docker "$USER"
  echo "  installed: $(docker --version)"
fi
sudo mkdir -p /opt/globoticket/logs /opt/globoticket/plugins /opt/globoticket/config
sudo chown -R "$USER":"$USER" /opt/globoticket
REMOTE

# --- ship the files -----------------------------------------------------------
# The Airflow containers run as uid 50000 and take ownership of dags/ on first start, so
# the old copies are removed with sudo before the new ones go up.
say "Copying the repo to /opt/globoticket"
$SSH "$VM_USER@$VM_IP" "sudo rm -rf /opt/globoticket/dags /opt/globoticket/assets /opt/globoticket/api"
$SCP -q -r "$HERE/dags" "$HERE/assets" "$HERE/api" "$VM_USER@$VM_IP:/opt/globoticket/"
$SCP -q "$HERE/Dockerfile" "$HERE/sitecustomize.py" "$HERE/docker-compose.yaml" "$ENV_FILE" \
  "$VM_USER@$VM_IP:/opt/globoticket/"
$SSH "$VM_USER@$VM_IP" "chmod 600 /opt/globoticket/.env"
echo "  dags, assets, api, Dockerfile, sitecustomize.py, docker-compose.yaml and .env"

# --- build and start ----------------------------------------------------------
# --force-recreate: the new dags/ and assets/ folders replace the old ones, and a running
# container keeps the old (deleted) folder mounted until it's recreated.
say "Building the image and starting the stack (the first build takes a few minutes)"
$SSH "$VM_USER@$VM_IP" "cd /opt/globoticket && sudo docker compose build -q && sudo docker compose up -d --force-recreate 2>&1 | tail -2"

# --- wait for health ----------------------------------------------------------
say "Waiting for Airflow to answer"
for i in $(seq 1 60); do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 6 "http://$VM_IP:8080/api/v2/monitor/health" 2>/dev/null || echo 000)
  if [ "$CODE" = "200" ]; then echo "  Airflow UI  http://$VM_IP:8080   HTTP 200"; break; fi
  echo "  ...health=$CODE"
  [ "$i" = "60" ] && { echo "Airflow did not come up. Check 'sudo docker compose ps' and 'logs' on the VM." >&2; exit 1; }
  sleep 8
done

# --- confirm the provider, unpause the Dags ----------------------------------
say "Checking the Common AI Provider and unpausing the course Dags (not the environment check, which script 3 runs on its own)"
$SSH "$VM_USER@$VM_IP" "cd /opt/globoticket && sudo docker compose exec -T airflow-scheduler bash -lc '
  airflow providers list 2>/dev/null | grep -E \"common-ai\" | sed \"s/^/  /\"
  airflow dags reserialize >/dev/null 2>&1 || true
  for d in \$(airflow dags list -o plain 2>/dev/null | awk \"NR>1 && /globoticket/ && !/check_environment/ {print \\\$1}\"); do
    airflow dags unpause \$d >/dev/null 2>&1 && echo \"  unpaused \$d\"
  done'"

say "Done - now run scripts/3_create_connections.sh"
cat <<EOF

  Airflow UI   http://$VM_IP:8080     login  airflow / airflow
  SSH          ssh -i $KEYFILE $VM_USER@$VM_IP

  The Dags parse fine right now - a missing Connection is a run-time failure, not an
  import error. Step 3 creates the Connections and then proves they work.

EOF
