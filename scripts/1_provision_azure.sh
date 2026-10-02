#!/usr/bin/env bash
#
# Step 1 of 3 - provision the Azure resources for the Globoticket AI pipeline.
#
# Companion repo for the Pluralsight course
# "Build AI and Agentic Pipelines with Apache Airflow 3".
#
# Re-runnable: every step checks for an existing resource first, so running this twice
# does not create duplicates and does not destroy data.
#
# Creates:
#   resource group      rg-globoticket-ai            in AZ_LOCATION (default canadacentral)
#   storage account     stgloboai<suffix>            StorageV2, hierarchical namespace ON (ADLS Gen2)
#   blob container      globoticket-intake
#   vnet + subnet + NSG  SSH and the Airflow UI restricted to YOUR public IP
#   VM                  vm-airflow                   Standard_B2as_v2, Ubuntu 22.04
#   Azure OpenAI        oai-globoticket-<suffix>     in OAI_LOCATION (default eastus2)
#   model deployment    gpt-4.1-mini                 GlobalStandard
#
# The event catalog database is PostgreSQL in a container on the VM, started by step 2,
# so there is no database server to pay for while the VM is deallocated.
#
# Writes every endpoint and secret to  .env  in the repo root. That file is git-ignored.
# NEVER COMMIT IT.
#
# Teardown, which is the only thing that stops the bill:
#   az group delete -n rg-globoticket-ai --yes --no-wait
#
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$HERE/.env"
KEY_FILE="$HERE/globoticket_vm_key"

# --- settings you may want to change -----------------------------------------
# Override any of these from the environment, e.g.
#   AZ_LOCATION=westeurope OAI_LOCATION=swedencentral ./scripts/1_provision_azure.sh
LOC="${AZ_LOCATION:-canadacentral}"
RG="${AZ_RESOURCE_GROUP:-rg-globoticket-ai}"

VM_NAME="${VM_NAME:-vm-airflow}"
# Standard_B2ms failed preflight in canadacentral with SkuNotAvailable (a regional capacity
# restriction, which a quota request does not fix). B2as_v2 is the same shape and is
# generally available. Not B2ps_v2 or D2ps_v5: those are ARM64 and the Airflow image is amd64.
VM_SIZE="${VM_SIZE:-Standard_B2as_v2}"
VM_IMAGE="Ubuntu2204"
VM_USER="${VM_USER:-azureuser}"

CONTAINER="globoticket-intake"

# The model can live in a different region from everything else. Check which regions give
# your subscription quota for it with:
#   az cognitiveservices usage list -l <region> --query "[?contains(name.value,'gpt4.1-mini')]"
# On the subscription this course was built on, canadacentral had none and eastus2 had plenty.
OAI_LOCATION="${OAI_LOCATION:-eastus2}"
OAI_MODEL="gpt-4.1-mini"
OAI_MODEL_VERSION="2025-04-14"
OAI_DEPLOYMENT="${OAI_DEPLOYMENT:-gpt-4.1-mini}"
OAI_CAPACITY="${OAI_CAPACITY:-50}"      # thousands of tokens per minute

say() { printf '\n\033[1m==> %s\033[0m\n' "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

command -v az >/dev/null || die "the Azure CLI is not on PATH. Install it, then 'az login'."

# --- which subscription? ------------------------------------------------------
# Passed explicitly on every command. If your az login can see more than one subscription,
# relying on whichever one happens to be active is how you create billable resources in
# the wrong place.
SUB="${AZ_SUBSCRIPTION:-$(command az account show --query id -o tsv 2>/dev/null || true)}"
[ -n "$SUB" ] || die "not logged in. Run 'az login', or set AZ_SUBSCRIPTION=<id>."

SUB_NAME=$(command az account show --subscription "$SUB" --query name -o tsv)
say "Subscription"
echo "  $SUB_NAME"
echo "  $SUB"
echo
# Set ASSUME_YES=1 to skip the prompt. Without it, a run with nothing on stdin reads EOF,
# and EOF must never read as consent.
if [ "${ASSUME_YES:-0}" = "1" ]; then
  echo "  ASSUME_YES=1, continuing without asking"
elif [ ! -t 0 ]; then
  die "no terminal on stdin, so nobody can answer the confirmation. Re-run attached to a terminal, or set ASSUME_YES=1 to create billable resources unattended."
else
  read -r -p "  Create billable resources in THIS subscription? [y/N] " ok
  [ "$ok" = "y" ] || [ "$ok" = "Y" ] || die "aborted."
fi

az() { command az "$@" --subscription "$SUB"; }

# --- resource providers -------------------------------------------------------
# A fresh subscription often has Microsoft.CognitiveServices unregistered, and the
# Azure OpenAI account create then fails with a registration error.
say "Resource providers"
for p in Microsoft.CognitiveServices Microsoft.Compute Microsoft.Network Microsoft.Storage; do
  if [ "$(az provider show -n "$p" --query registrationState -o tsv)" != "Registered" ]; then
    echo "  registering $p (a minute or two)"
    az provider register -n "$p" --wait
  fi
  echo "  $p registered"
done

# --- your public IP, for the NSG ----------------------------------------------
say "Detecting your public IP"
MY_IP="${ALLOWED_IP:-$(curl -s https://api.ipify.org || true)}"
printf '%s' "$MY_IP" | grep -Eq '^[0-9]+(\.[0-9]+){3}$' \
  || die "could not determine your public IP. Set ALLOWED_IP=<your ip> and re-run."
echo "  $MY_IP  (SSH and the Airflow UI will be restricted to this)"
echo "  If your ISP rotates it, re-run this script - the NSG rules are updated in place."

# --- unique suffix ------------------------------------------------------------
# Storage and Azure OpenAI names are globally unique.
SUFFIX=$(printf '%s' "$SUB" | tr -d '-' | cut -c1-6)
SA="stgloboai${SUFFIX}"
OAI="oai-globoticket-${SUFFIX}"

# --- resource group -----------------------------------------------------------
say "Resource group: $RG"
if az group exists -n "$RG" | grep -q true; then echo "  exists"; else
  az group create -n "$RG" -l "$LOC" -o none; echo "  created"
fi

# --- storage account + container ---------------------------------------------
# Hierarchical namespace ON makes this ADLS Gen2, which is what abfs:// targets.
# ObjectStoragePath's Azure filesystem registers abfs, abfss and adl - not wasb. A wasb://
# path does not error, it silently writes to the worker's own disk.
say "Storage account: $SA (hierarchical namespace enabled)"
if az storage account show -n "$SA" -g "$RG" -o none 2>/dev/null; then echo "  exists"; else
  az storage account create \
    -n "$SA" -g "$RG" -l "$LOC" \
    --sku Standard_LRS --kind StorageV2 \
    --enable-hierarchical-namespace true \
    --min-tls-version TLS1_2 \
    --allow-blob-public-access false \
    -o none
  echo "  created"
fi
SA_KEY=$(az storage account keys list -n "$SA" -g "$RG" --query "[0].value" -o tsv)

say "Blob container: $CONTAINER"
if az storage container exists --account-name "$SA" --account-key "$SA_KEY" -n "$CONTAINER" --query exists -o tsv | grep -q true; then
  echo "  exists"
else
  az storage container create --account-name "$SA" --account-key "$SA_KEY" -n "$CONTAINER" -o none
  echo "  created"
fi

# --- Azure OpenAI + the model deployment ---------------------------------------
say "Azure OpenAI: $OAI in $OAI_LOCATION"
if az cognitiveservices account show -n "$OAI" -g "$RG" -o none 2>/dev/null; then echo "  exists"; else
  az cognitiveservices account create \
    -n "$OAI" -g "$RG" -l "$OAI_LOCATION" \
    --kind OpenAI --sku S0 --custom-domain "$OAI" --yes -o none
  echo "  created"
fi

say "Model deployment: $OAI_DEPLOYMENT ($OAI_MODEL $OAI_MODEL_VERSION, GlobalStandard, ${OAI_CAPACITY}K TPM)"
if az cognitiveservices account deployment show -n "$OAI" -g "$RG" --deployment-name "$OAI_DEPLOYMENT" -o none 2>/dev/null; then
  echo "  exists"
else
  az cognitiveservices account deployment create \
    -n "$OAI" -g "$RG" --deployment-name "$OAI_DEPLOYMENT" \
    --model-name "$OAI_MODEL" --model-version "$OAI_MODEL_VERSION" --model-format OpenAI \
    --sku-name GlobalStandard --sku-capacity "$OAI_CAPACITY" -o none \
    || die "the deployment failed. The usual cause is no $OAI_MODEL quota in $OAI_LOCATION for this subscription. Try another OAI_LOCATION."
  echo "  created"
fi
OAI_ENDPOINT=$(az cognitiveservices account show -n "$OAI" -g "$RG" --query properties.endpoint -o tsv)
OAI_KEY=$(az cognitiveservices account keys list -n "$OAI" -g "$RG" --query key1 -o tsv)

# --- network ------------------------------------------------------------------
say "Network: vnet-globoticket / nsg-airflow"
if ! az network vnet show -g "$RG" -n vnet-globoticket -o none 2>/dev/null; then
  az network vnet create -g "$RG" -n vnet-globoticket \
    --address-prefix 10.42.0.0/16 \
    --subnet-name snet-airflow --subnet-prefix 10.42.1.0/24 -o none
  echo "  vnet created"
else
  echo "  vnet exists"
fi

if ! az network nsg show -g "$RG" -n nsg-airflow -o none 2>/dev/null; then
  az network nsg create -g "$RG" -n nsg-airflow -o none; echo "  nsg created"
else
  echo "  nsg exists"
fi

# Everything is open to YOUR IP ONLY. Never put 0.0.0.0/0 here: this Airflow UI has the
# default credentials and a public one is a real security incident, not a demo.
add_rule() { # name priority port
  az network nsg rule create -g "$RG" --nsg-name nsg-airflow -n "$1" \
    --priority "$2" --source-address-prefixes "$MY_IP" --destination-port-ranges "$3" \
    --access Allow --protocol Tcp --direction Inbound -o none 2>/dev/null \
  || az network nsg rule update -g "$RG" --nsg-name nsg-airflow -n "$1" \
    --source-address-prefixes "$MY_IP" -o none
}
add_rule allow-ssh     100 22
add_rule allow-airflow 110 8080
# Report what the rules carry, not what we assume they carry.
RULE_IP=$(az network nsg rule show -g "$RG" --nsg-name nsg-airflow -n allow-airflow --query sourceAddressPrefix -o tsv)
echo "  rules allow $RULE_IP (ssh 22, airflow 8080)"

az network vnet subnet update -g "$RG" --vnet-name vnet-globoticket -n snet-airflow \
  --network-security-group nsg-airflow -o none
echo "  nsg attached to subnet"

# --- ssh key ------------------------------------------------------------------
say "SSH key"
if [ -f "$KEY_FILE" ]; then
  echo "  reusing $KEY_FILE"
else
  ssh-keygen -t rsa -b 4096 -f "$KEY_FILE" -N "" -C "globoticket-ai" >/dev/null
  chmod 600 "$KEY_FILE" 2>/dev/null || true
  echo "  generated $KEY_FILE (git-ignored)"
fi

# --- vm -----------------------------------------------------------------------
say "VM: $VM_NAME ($VM_SIZE)"
if az vm show -g "$RG" -n "$VM_NAME" -o none 2>/dev/null; then echo "  exists"; else
  az vm create \
    -g "$RG" -n "$VM_NAME" -l "$LOC" \
    --image "$VM_IMAGE" --size "$VM_SIZE" \
    --admin-username "$VM_USER" \
    --ssh-key-values "${KEY_FILE}.pub" \
    --vnet-name vnet-globoticket --subnet snet-airflow \
    --nsg "" \
    --public-ip-sku Standard \
    --os-disk-size-gb 64 \
    -o none
  echo "  created"
fi
VM_IP=$(az vm show -d -g "$RG" -n "$VM_NAME" --query publicIps -o tsv)
PRIV_IP=$(az vm show -d -g "$RG" -n "$VM_NAME" --query privateIps -o tsv)
echo "  public ip: $VM_IP    private ip: $PRIV_IP"

# --- secrets for the stack ----------------------------------------------------
# Generated once and reused, so re-running this script does not invalidate the Connections
# you already created or the Fernet-encrypted rows in the metadata database.
prev() { grep -E "^$1=" "$ENV_FILE" 2>/dev/null | cut -d= -f2- || true; }
PG_PASS="$(prev PG_PASSWORD)";   [ -n "$PG_PASS" ]   || PG_PASS="Gt$(openssl rand -hex 16)"
API_TOKEN="$(prev GLOBOTICKET_API_TOKEN)"; [ -n "$API_TOKEN" ] || API_TOKEN="gt_$(openssl rand -hex 24)"
FERNET_KEY="$(prev FERNET_KEY)"; [ -n "$FERNET_KEY" ] || FERNET_KEY="$(openssl rand -base64 32)"

# --- write .env ---------------------------------------------------------------
say "Writing $ENV_FILE"
cat > "$ENV_FILE" <<EOF
# Globoticket demo environment - GENERATED by scripts/1_provision_azure.sh.
# Contains live credentials. This file is git-ignored. DO NOT COMMIT IT.
AZ_SUBSCRIPTION=$SUB
AZ_LOCATION=$LOC
AZ_RESOURCE_GROUP=$RG

STORAGE_ACCOUNT=$SA
STORAGE_KEY=$SA_KEY
STORAGE_CONTAINER=$CONTAINER

# The v1 endpoint is used, so the Connection needs no API version
OAI_ENDPOINT=${OAI_ENDPOINT%/}/openai/v1
OAI_KEY=$OAI_KEY
OAI_DEPLOYMENT=$OAI_DEPLOYMENT

VM_NAME=$VM_NAME
VM_PUBLIC_IP=$VM_IP
VM_PRIVATE_IP=$PRIV_IP
VM_USER=$VM_USER
VM_SSH_KEY=$KEY_FILE
AIRFLOW_URL=http://$VM_IP:8080

# The event catalog: PostgreSQL in a container on the VM
PG_DATABASE=globoticket
PG_USER=globo
PG_PASSWORD=$PG_PASS

ALLOWED_IP=$MY_IP

# Consumed by docker-compose on the VM
AIRFLOW_UID=50000
FERNET_KEY=$FERNET_KEY
GLOBOTICKET_API_TOKEN=$API_TOKEN
EOF
chmod 600 "$ENV_FILE" 2>/dev/null || true

say "Done - now run scripts/2_install_stack.sh"
cat <<EOF

  SSH             ssh -i $KEY_FILE $VM_USER@$VM_IP
  Storage         $SA, container $CONTAINER
  Model           $OAI_DEPLOYMENT on $OAI ($OAI_LOCATION)

  Credentials are in $ENV_FILE (chmod 600, git-ignored).

  Cost control - the VM bills by the hour while it's allocated:
    stop the vm   az vm deallocate -g $RG -n $VM_NAME --subscription $SUB
    start the vm  az vm start      -g $RG -n $VM_NAME --subscription $SUB
    delete it all az group delete  -n $RG --yes --no-wait --subscription $SUB

EOF
