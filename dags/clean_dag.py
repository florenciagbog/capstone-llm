import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "description": "Clean StackOverflow data for the LLM capstone project",
    "depend_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "capstonellm_clean",
    default_args=default_args,
    schedule=None,
    catchup=False,
) as dag:
    clean = DockerOperator(
        task_id="clean",
        image="capstonellm-clean",
        container_name="capstonellm_clean",
        api_version="auto",
        auto_remove="force",
        command=["--env", "docker", "--tag", "dbt"],
        environment={
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )
