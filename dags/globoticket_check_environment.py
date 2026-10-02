"""Prove the environment works: one model call, one storage listing, one database query.

scripts/3_create_connections.sh runs this after creating the Connections. It is a setup
check, not part of the course, so it is tagged `setup` and drops out of a `globoticket`
tag filter in the Dags list.
"""
import pendulum

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import ObjectStoragePath, dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    tags=["setup"],
)
def globoticket_check_environment():
    ask_model = LLMOperator(
        task_id="ask_model",
        llm_conn_id="globoticket_llm",
        prompt="Reply with the single word: ok",
    )

    @task
    def report_model(reply: str) -> None:
        print(f"Model ok - globoticket_llm answered {reply.strip()!r}")

    @task
    def check_storage() -> None:
        root = ObjectStoragePath("abfs://globoticket-intake/", conn_id="globoticket_blob")
        n = len(list(root.iterdir()))
        print(f"Storage ok - abfs://globoticket-intake/ reachable, {n} item(s) at the top level")

    @task
    def check_postgres() -> None:
        version = PostgresHook(postgres_conn_id="globoticket_pg").get_first("SELECT version()")[0]
        print(f"PostgreSQL ok - {version.split(',')[0]}")

    report_model(ask_model.output)
    check_storage()
    check_postgres()


globoticket_check_environment()
