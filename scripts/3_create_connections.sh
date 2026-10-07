#!/usr/bin/env bash
#
# Step 3 of 3 - create the Airflow Connections the course uses, then prove they work.
#
# Run this from your machine, from the repo root, after 2_install_stack.sh.
# Re-runnable: it deletes and recreates every Connection, so it is also the fix for
# "I edited a Connection in the UI and now I want the original back".
#
#   globoticket_llm    pydanticai_azure   the GPT model deployment in Microsoft Foundry
#   globoticket_blob   adls               the intake container in Azure storage
#   globoticket_pg     postgres           the event catalog database on the VM
#   globoticket_api    http               the read-only bookings API on the VM (module 3)
#
# It also loads the event catalog into globoticket-pg and uploads the event brief files
# in assets/event-briefs/ to Blob Storage.
#
# The values come from .env. Nothing is typed by hand and nothing is committed.
#
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$HERE/.env"
[ -f "$ENV_FILE" ] || { echo "ERROR: no $ENV_FILE - run scripts/1_provision_azure.sh first" >&2; exit 1; }

get() { grep -E "^$1=" "$ENV_FILE" | cut -d= -f2-; }
VM_IP=$(get VM_PUBLIC_IP); VM_USER=$(get VM_USER); KEYFILE=$(get VM_SSH_KEY)
OAI_ENDPOINT=$(get OAI_ENDPOINT); OAI_KEY=$(get OAI_KEY); OAI_DEPLOYMENT=$(get OAI_DEPLOYMENT)
SA=$(get STORAGE_ACCOUNT); KEY=$(get STORAGE_KEY)
PGDB=$(get PG_DATABASE); PGUSER=$(get PG_USER); PGPASS=$(get PG_PASSWORD)
APITOKEN=$(get GLOBOTICKET_API_TOKEN)

SSH="ssh -i $KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
SCP="scp -i $KEYFILE -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR"
say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }

# The values never appear in an argument list on the VM: they are passed as environment
# variables to a script that runs inside the scheduler container, so they do not land in
# the VM's shell history or in `ps` output.
TMP=$(mktemp)
cat > "$TMP" <<'REMOTE'
#!/bin/bash
set -e
for c in globoticket_llm globoticket_blob globoticket_pg globoticket_api; do
  airflow connections delete "$c" >/dev/null 2>&1 || true
done

# The model. The connection type comes from the Common AI Provider.
#   host     = the Foundry resource's v1 endpoint (https://<resource>.openai.azure.com/openai/v1)
#   password = the API key, which Airflow masks as *** everywhere
#   extra    = the model, as azure:<deployment name>, and api_version
# api_version is stored as null on purpose. The v1 endpoint doesn't take one, and the hook
# only passes it when it has a value. Storing the key at all is what makes the Edit
# Connection form show an empty "API Version" field: the form renders only the extra
# fields a Connection actually stores. Set a value only for an older, dated endpoint.
airflow connections add globoticket_llm \
  --conn-type pydanticai_azure \
  --conn-host "${OAI_ENDPOINT}" \
  --conn-password "${OAI_KEY}" \
  --conn-extra "{\"model\": \"azure:${OAI_DEPLOYMENT}\", \"api_version\": null}" >/dev/null
echo "  globoticket_llm    pydanticai_azure  azure:${OAI_DEPLOYMENT}"

# The account KEY goes in the PASSWORD field, not in Extra. Airflow redacts `password`
# unconditionally, but redacts `extra` key by key against DEFAULT_SENSITIVE_FIELDS, which
# has `access_key` and NOT `account_key`, so a key in Extra renders in cleartext.
# The Azure filesystem reads it from `password` (fs/adls.py: options["account_key"] = password).
# `account_name` stays in Extra because adlfs needs it under that exact name.
airflow connections add globoticket_blob \
  --conn-type adls \
  --conn-host "${SA}" \
  --conn-password "${KEY}" \
  --conn-extra "{\"account_name\":\"${SA}\"}" >/dev/null
echo "  globoticket_blob   adls              ${SA}"

# The catalog database runs in the globoticket-pg container on the same Docker network.
airflow connections add globoticket_pg \
  --conn-type postgres \
  --conn-host globoticket-pg --conn-port 5432 \
  --conn-schema "${PGDB}" \
  --conn-login "${PGUSER}" --conn-password "${PGPASS}" >/dev/null
echo "  globoticket_pg     postgres          globoticket-pg/${PGDB}"

# The read-only bookings API runs in the globoticket-api container on the same network.
# The token travels as an Authorization header from Extra, which Airflow masks as ***.
airflow connections add globoticket_api \
  --conn-type http \
  --conn-host globoticket-api --conn-port 8000 --conn-schema http \
  --conn-extra "{\"Authorization\": \"Bearer ${APITOKEN}\"}" >/dev/null
echo "  globoticket_api    http              globoticket-api:8000"

# The event catalog: 120 synthetic events, loaded fresh every run so a reset always
# starts from the same catalog. It lives in globoticket-pg, not in Airflow's database.
python - <<'PY'
# A hook can't look up a Connection outside a running task in Airflow 3, so this
# connects with psycopg2 directly, using the same values the Connection holds.
import json, os
import psycopg2
events = json.load(open("/opt/airflow/assets/catalog/events.json", encoding="utf-8"))
cols = ["event_id", "name", "act", "category", "venue", "city", "country", "capacity",
        "base_price", "currency", "status", "promoter", "event_date"]
with psycopg2.connect(host="globoticket-pg", dbname=os.environ["PGDB"],
                      user=os.environ["PGUSER"], password=os.environ["PGPASS"]) as conn:
    with conn.cursor() as cur:
        cur.execute("""
          DROP TABLE IF EXISTS events;
          CREATE TABLE events (
            event_id text PRIMARY KEY, name text, act text, category text, venue text, city text,
            country text, capacity int, base_price numeric, currency text, status text,
            promoter text, event_date date);
        """)
        cur.executemany(f"INSERT INTO events ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})",
                        [[e[c] for c in cols] for e in events])
print(f"  event catalog      {len(events)} events loaded into globoticket-pg")
PY
REMOTE

say "Creating the Connections"
$SCP -q "$TMP" "$VM_USER@$VM_IP:/tmp/_mkconns.sh"
rm -f "$TMP"
$SSH "$VM_USER@$VM_IP" "cd /opt/globoticket && \
  sudo docker compose cp /tmp/_mkconns.sh airflow-scheduler:/tmp/_mkconns.sh >/dev/null 2>&1 && \
  sudo docker compose exec -T \
    -e OAI_ENDPOINT='$OAI_ENDPOINT' -e OAI_KEY='$OAI_KEY' -e OAI_DEPLOYMENT='$OAI_DEPLOYMENT' \
    -e SA='$SA' -e KEY='$KEY' \
    -e PGDB='$PGDB' -e PGUSER='$PGUSER' -e PGPASS='$PGPASS' -e APITOKEN='$APITOKEN' \
    airflow-scheduler bash /tmp/_mkconns.sh"
$SSH "$VM_USER@$VM_IP" "rm -f /tmp/_mkconns.sh"

# --- the event brief files ---------------------------------------------------
# Module 2 reads event briefs from Blob Storage. The files live in assets/event-briefs/
# in this repo; this copies them to the intake container's event-briefs/ folder.
# --overwrite makes it safe to re-run, which is how reset_demo.sh puts them back.
say "Uploading the event brief files"
CONTAINER=$(get STORAGE_CONTAINER)
az storage blob upload-batch   --account-name "$SA" --account-key "$KEY"   --destination "$CONTAINER" --destination-path event-briefs   --source "$HERE/assets/event-briefs" --pattern "*.md" --overwrite   --only-show-errors --no-progress -o none
for f in "$HERE"/assets/event-briefs/*.md; do echo "  event-briefs/$(basename "$f")"; done

# --- prove they work ----------------------------------------------------------
# NOT the Test Connection button. On the Azure OpenAI connection type it only resolves the
# model name and never calls it, so it passes with a wrong key. This Dag calls all four
# systems for real: one tiny model request, a container listing, a database query and an
# authenticated bookings API call.
say "Verifying with the globoticket_check_environment Dag"
# `airflow dags test` passes a task's print() through only some of the time, so the
# verdict comes from the task states Airflow recorded, not from grepping the output.
VERIFY=$(mktemp)
cat > "$VERIFY" <<'REMOTE'
#!/bin/bash
# Keep this Dag paused. `airflow dags test` runs it in-process, and if the Dag is unpaused
# the scheduler picks up the same run too, so tasks run twice and race each other.
airflow dags pause globoticket_check_environment >/dev/null 2>&1 || true
airflow dags test globoticket_check_environment >/dev/null 2>&1
RID=$(airflow dags list-runs globoticket_check_environment -o json 2>/dev/null \
      | python -c 'import json,sys; print(json.load(sys.stdin)[0]["run_id"])')
airflow tasks states-for-dag-run globoticket_check_environment "$RID" -o json 2>/dev/null \
  | python -c '
import json, sys
what = {"ask_model": "model call through globoticket_llm",
        "report_model": "model reply read back from XCom",
        "check_storage": "listing abfs://globoticket-intake/ through globoticket_blob",
        "check_postgres": "query through globoticket_pg",
        "check_api": "authenticated call through globoticket_api"}
rows = json.load(sys.stdin)
for r in sorted(rows, key=lambda r: r["task_id"]):
    state, tid = r["state"], r["task_id"]
    print("  %-8s %-15s %s" % (state, tid, what.get(tid, "")))
bad = [r for r in rows if r["state"] != "success"]
print("  ALL OK" if not bad else f"  {len(bad)} task(s) did not succeed")
'
REMOTE
$SCP -q "$VERIFY" "$VM_USER@$VM_IP:/tmp/_verify.sh"
rm -f "$VERIFY"
$SSH "$VM_USER@$VM_IP" "cd /opt/globoticket && \
  sudo docker compose cp /tmp/_verify.sh airflow-scheduler:/tmp/_verify.sh >/dev/null 2>&1 && \
  sudo docker compose exec -T airflow-scheduler bash /tmp/_verify.sh; rm -f /tmp/_verify.sh" || true

cat <<EOF

  If every task shows success and the last line says ALL OK, the environment
  is ready and you can follow along with the course.

  If the model line failed      check OAI_ENDPOINT and OAI_KEY in .env, and that the
                                deployment exists: az cognitiveservices account deployment list
  If storage failed             confirm the storage account has hierarchical namespace ON.
                                Without it, abfs:// cannot resolve.
  If PostgreSQL failed          check 'sudo docker compose ps' on the VM shows globoticket-pg healthy

  Airflow UI   http://$VM_IP:8080     login  airflow / airflow

  When you stop for the day:
    az vm deallocate -g $(get AZ_RESOURCE_GROUP) -n $(get VM_NAME) --subscription $(get AZ_SUBSCRIPTION)

EOF
