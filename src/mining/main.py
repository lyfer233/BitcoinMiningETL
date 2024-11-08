import logging
import time
import traceback
from datetime import timedelta, datetime

import pendulum
import asyncio
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule
from mining.utils.extract import fetch_data_from_api
from mining.utils.transform import api_trans, mysql_trans
from mining.utils.load import load_mysql_query, load_mysql_add

# DAG parameter
DAG_SCHEDULE_INTERVAL = timedelta(seconds=10)
FETCH_PRICE_INTERVAL = 60
FETCH_HASHRATE_INTERVAL = 30
LOAD_INTERVAL = 300
# URL
PRICE_URL = "https://mempool.space/api/v1/prices"
HASH_RATE_URL = "https://mempool.space/api/v1/mining/hashrate/24h"


def send_failure_email(context):
    try:
        failure_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        task_instance = context.get("task_instance")
        task_id = task_instance.task_id if task_instance else "Unknown"
        dag_id = context.get("dag").dag_id if context.get("dag") else "Unknown"

        log_url = task_instance.log_url if task_instance else "No log URL available"

        html_content = f"""
        <h3>Airflow Task Failure Notification</h3>
        <p><strong>Time of Failure:</strong> {failure_time}</p>
        <p><strong>DAG ID:</strong> {dag_id}</p>
        <p><strong>Task ID:</strong> {task_id}</p>
        <p><strong>Execution Date:</strong> {context.get("execution_date")}</p>
        <p><strong>Log URL:</strong> <a href="{log_url}">{log_url}</a></p>
        <p>Task failed in DAG: {dag_id}. Please check the logs for more details.</p>
        """

        logging.info("Sending failure email...")
        send_email(
            to='stoner.ft.tech@gmail.com',
            subject=f'Airflow Task Failure: {context["task_instance_key_str"]}',
            html_content=html_content,
            conn_id='233'
        )
        logging.info("Failure email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send failure email: {e}")


@dag(
    dag_id="bitcoin_mining",
    schedule=DAG_SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2024, 11, 1),
    catchup=False,
    tags=["mining"],
    on_failure_callback=send_failure_email,
    params={
        "FetchPriceInterval": FETCH_PRICE_INTERVAL,
        "FetchHashrateInterval": FETCH_HASHRATE_INTERVAL,
        "LoadInterval": LOAD_INTERVAL
    }
)
def bitcoin_mining_record():
    @task(
        multiple_outputs=True,
        retries=2,
        retry_delay=timedelta(seconds=5),
        on_failure_callback=send_failure_email
    )
    def fetch_price_from_api(params: dict = None):
        """
        #### get price data from api
        """
        last_run_time = int(Variable.get("last_price_task_run", default_var="0"))
        spider_ts = int(time.time())
        fetch_price_interval = params["FetchPriceInterval"]

        if (spider_ts - last_run_time) >= fetch_price_interval:
            Variable.set("last_price_task_run", spider_ts)
            logging.info("fetch price start~")

            data = asyncio.run(fetch_data_from_api(PRICE_URL, 5))

            return {
                "spider_ts": spider_ts,
                "price_data": data,
            }
        else:
            return None

    @task(
        multiple_outputs=True,
        retries=2,
        retry_delay=timedelta(seconds=5),
        on_failure_callback=send_failure_email
    )
    def fetch_hash_rate_from_api(params: dict = None):
        """
        #### get hash rate and difficuty from api
        """
        last_run_time = int(Variable.get("last_hash_rate_task_run", default_var="0"))
        spider_ts = int(time.time())
        fetch_hash_rate_interval = params["FetchHashrateInterval"]

        if (spider_ts - last_run_time) >= fetch_hash_rate_interval:
            Variable.set("last_hash_rate_task_run", spider_ts)
            logging.info("fetch hash rate start~")

            data = asyncio.run(fetch_data_from_api(HASH_RATE_URL, 5))

            return {
                "spider_ts": spider_ts,
                "hash_rate_data": data,
            }
        else:
            return None

    @task(
        trigger_rule=TriggerRule.ONE_SUCCESS,
        on_failure_callback=send_failure_email
    )
    def transform(price_data: dict = None, hash_rate_data: dict = None, params: dict = None):
        """
        #### Transform task
        Transform data from API
        """
        transformed_data = api_trans(price_data, hash_rate_data)
        try:
            mysql_trans(transformed_data,
                        MySqlHook(mysql_conn_id="mysql_mining").get_conn())
        except Exception as e:
            raise RuntimeError(
                f"MySQL data save failed. The detail error info is: "
                f"{e}\n{traceback.format_exc()}"
            )

    @task(on_failure_callback=send_failure_email)
    def load(params: dict = None):
        """
        #### Load task
        Generate a dictionary with averages for BTC/USD price, Difficulty,
        and Hashrate in 5-minute intervals.
        """
        load_interval = params["LoadInterval"]
        last_run_time = int(Variable.get("last_load_task_run", default_var="0"))
        spider_ts = int(time.time())

        if (spider_ts - last_run_time) >= load_interval:
            Variable.set("last_load_task_run", spider_ts)
            logging.info("Load task starts.")

            try:
                mysql_hook = MySqlHook(mysql_conn_id="mysql_mining").get_conn()
                data = load_mysql_query({
                    "spider_ts": spider_ts,
                    "load_interval": load_interval
                }, mysql_hook)
                logging.info(f"load_mysql_query: {data}")
                load_mysql_add(data, mysql_hook)

            except Exception as e:
                raise RuntimeError(
                    f"Load data failed. The detail error info is:"
                    f" {e}\n{traceback.format_exc()}"
                )

            return {
                "spider_ts": spider_ts,
                "price": data['avg_price'],
                "hashrate": data['avg_hashrate'],
                "difficulty": data['avg_difficulty']
            }

        else:
            logging.info("Load task skipped due to insufficient interval.")

    # set the dependencies
    price_data = fetch_price_from_api()
    hash_rate_data = fetch_hash_rate_from_api()
    transform(price_data, hash_rate_data) >> load()


bitcoin_mining_record()

if __name__ == "__main__":
    bitcoin_mining_record().test()
