# The Globoticket Airflow image: the official Airflow 3.3.2 image plus the Common AI Provider.
#
# The stock apache/airflow image doesn't include the Common AI Provider, so this adds it.
# The version is pinned so every environment built from this repo gets exactly the same one.
# The [openai] extra pulls in the OpenAI client that the Azure OpenAI connection type uses.
FROM apache/airflow:3.3.2

RUN pip install --no-cache-dir \
      "apache-airflow==3.3.2" \
      "apache-airflow-providers-common-ai[openai,sql]==0.10.0"

# Works around an event-loop clash between adlfs and pydantic-ai. See the file for details.
COPY sitecustomize.py /home/airflow/.local/lib/python3.13/site-packages/sitecustomize.py
