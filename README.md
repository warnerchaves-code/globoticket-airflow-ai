# Globoticket: Airflow 3 AI and agentic pipelines

Companion repo for the Pluralsight course **Build AI and Agentic Pipelines with Apache Airflow 3**.

Everything here builds the environment the demos run in, so you can follow along in your own Azure
subscription rather than watching. Three scripts, about twenty minutes, most of it waiting for Azure.

## The case study

Globoticket sells tickets for live events, and promoters run the events. When a promoter plans an
event, they write one brief and send it to every ticketing partner they work with. Globoticket is one
of those partners. Today the intake team reads each brief and retypes it into the event catalog. In
this course an LLM task takes over the retyping, inside an ordinary Airflow pipeline, and the intake
team keeps the final approval.

| What | Where it lives | Connection ID |
|---|---|---|
| The model | a GPT deployment in Microsoft Foundry (Azure OpenAI) | `globoticket_llm` |
| Event briefs and review packets | Azure Data Lake Storage Gen2 | `globoticket_blob` |
| The event catalog | PostgreSQL 16, in a container on the VM | `globoticket_pg` |

---

## What it costs

**This creates billable Azure resources.** The VM bills by the hour whenever it is allocated, and the
model bills per token. The storage account and a deallocated VM's disk cost very little.

Deallocate the VM when you stop for the day:

```bash
az vm deallocate -g rg-globoticket-ai -n vm-airflow
```

And when you finish the course, delete everything:

```bash
az group delete -n rg-globoticket-ai --yes --no-wait
```

---

## Before you start

- An Azure subscription you are willing to create resources in, with quota for `gpt-4.1-mini`
  (GlobalStandard) in at least one region. Step 1 explains how to check.
- The [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli), logged in with `az login`
- `bash`, `ssh`, `curl` and `openssl`. On Windows, Git Bash or WSL
- No Docker needed locally. It is installed on the VM for you

**About the model:** Microsoft retires `gpt-4.1-mini` for inference on 2027-04-14. After that date, set
`OAI_DEPLOYMENT`, and the model name and version in `scripts/1_provision_azure.sh`, to a current model.
Nothing in the Dags changes: they only name the `globoticket_llm` Connection.

---

## Setup

Run these from the repo root, in order.

```bash
./scripts/1_provision_azure.sh      # resource group, storage, Azure OpenAI + model, network, VM
./scripts/2_install_stack.sh        # Docker, the Globoticket image, Airflow 3.3.2, the catalog database
./scripts/3_create_connections.sh   # the three Connections, then proves they work
```

Step 1 asks you to confirm the subscription before it creates anything, and it writes every endpoint and
credential to `.env` in the repo root. **`.env` is git-ignored and holds live secrets. Never commit it.**
`.env.example` shows you its shape without you having to run anything.

The model can live in a different region from everything else, because model quota varies by region.
The default is `eastus2`. To use another one:

```bash
OAI_LOCATION=swedencentral ./scripts/1_provision_azure.sh
```

Step 3 finishes by running the `globoticket_check_environment` Dag and reporting each task. If every line
says `success` and the last one says `ALL OK`, you are ready:

```
  success  ask_model       model call through globoticket_llm
  success  check_postgres  query through globoticket_pg
  success  check_storage   listing abfs://globoticket-intake/ through globoticket_blob
  success  report_model    model reply read back from XCom
  ALL OK
```

Then open the Airflow UI at the address step 3 prints, and log in with `airflow` / `airflow`.

### Security, briefly

SSH and the Airflow UI are opened **to your public IP only**. This Airflow has the default credentials,
so never widen those rules to `0.0.0.0/0`. If your ISP rotates your address, re-run step 1: it updates
the rules in place rather than adding more.

### Replaying a demo

```bash
./scripts/reset_demo.sh
```

It starts the VM if you deallocated it, deletes the run history of the course Dags, and recreates the
Connections.

---

## What is in here

```
Dockerfile                          apache/airflow:3.3.2 plus the Common AI Provider, pinned
sitecustomize.py                    a fix for an adlfs / pydantic-ai event-loop clash (see below)
docker-compose.yaml                 Airflow 3.3.2 from the official quickstart file, plus the catalog database
assets/promoter_request.txt         the promoter email module 1 works with
dags/
  globoticket_models.py             EventRequest, the typed output the LLM tasks return
  globoticket_request_summary.py    module 1 - an LLMOperator summarizes the request
  globoticket_request_extract.py    module 1 - @task.llm returns a typed EventRequest
  globoticket_check_environment.py  setup check that step 3 runs
scripts/                            the three setup scripts and the reset
```

More Dags arrive as the course goes on.

### The Dags only name a Connection

Every Dag names the `globoticket_llm` Connection and nothing else. The endpoint, the API key and the model
deployment all live in the Connection. You can `grep` the Dags to check.

### Typed output needs one setting

`globoticket_request_extract` passes an `EventRequest` object from one task to the next through XCom.
Airflow only rebuilds classes it has been told to trust, so `docker-compose.yaml` sets:

```yaml
AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES: 'airflow.* globoticket_models.*'
```

Without it, the downstream task fails with `... was not found in allow list for deserialization imports`.
This applies even when both tasks are in the same Dag.

---

## Four things worth knowing before you hit them

### Test Connection passes with a wrong key

On the Azure OpenAI connection type, the Test Connection button only resolves the model name. It never
calls the model, so it reports success with a wrong key. Step 3 proves the Connection with a real model
call instead.

### The v1 endpoint doesn't take an API version

`globoticket_llm` points at the resource's v1 endpoint (`https://<resource>.openai.azure.com/openai/v1`),
which is why its API Version field is empty. You would set it only for an older, dated endpoint.

Step 3 stores `"api_version": null` rather than leaving the key out. The Edit Connection form only shows
the extra fields a Connection actually stores, so without it the empty API Version field wouldn't appear.

### The model is in its own region

On the subscription this course was built on, `canadacentral` had no `gpt-4.1-mini` quota and `eastus2`
had plenty. That's why `OAI_LOCATION` is separate from `AZ_LOCATION`. GlobalStandard deployments route
requests globally anyway.

### Why `sitecustomize.py` exists

adlfs, the library behind `abfs://`, installs fsspec's background event loop as the current loop of the
thread that connects. pydantic-ai, which every Common AI Provider task uses, later finds that loop and
fails with `RuntimeError: This event loop is already running`. The result is that
`LLMFileAnalysisOperator` can't read a file from Azure storage. `sitecustomize.py` stops adlfs from setting
the loop, and Python loads it automatically in every Airflow process in the image.

---

## Versions this was built and verified against

| | |
|---|---|
| Airflow | 3.3.2 |
| Common AI Provider | `apache-airflow-providers-common-ai` 0.10.0, with the `openai` and `sql` extras |
| Model | `gpt-4.1-mini`, version 2025-04-14, GlobalStandard |
| PostgreSQL | 16, in a container |
| VM | Standard_B2as_v2, Ubuntu 22.04 |

`Standard_B2ms` is the obvious VM size and it failed preflight in `canadacentral` with a capacity
restriction, which a quota request does not fix. `B2as_v2` is the same shape and generally available.
Avoid `B2ps_v2` and `D2ps_v5`: those are ARM64, and the Airflow image is amd64.

---

## License

Apache License 2.0. See [LICENSE](LICENSE).

`docker-compose.yaml` is a modified copy of the Apache Airflow project's quickstart compose file and keeps
its original ASF license header. The changes made to it are listed in [NOTICE](NOTICE).
