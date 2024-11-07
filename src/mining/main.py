import asyncio
import logging
import time
import traceback
from datetime import timedelta, datetime

import aiohttp
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.email import send_email

from mining.data_model import Price, Hashrate
from mining.utils.extract import check_task_interval, fetch
from mining.utils.transform import api_trans, mysql_trans
from mining.utils.load import load_mysql_query, load_mysql_add


def send_failure_email(context):
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

    send_email(
        to='stoner.ft.tech@gmail.com',
        subject=f'Airflow Task Failure: {context["task_instance_key_str"]}',
        html_content=html_content
    )


@dag(
    dag_id="bitcoin_mining",
    schedule=timedelta(seconds=10),
    start_date=pendulum.datetime(2024, 11, 1),
    catchup=False,
    tags=["mining"],
    on_failure_callback=send_failure_email,
    params={
        "ApiAlarmThreshold": 5,
        "FetchPriceInterval": 60,
        "FetchHashrateInterval": 60,
        "LoadInterval": 300
    }
)
def bitcoin_mining_record():
    """
    """
    @task(
        multiple_outputs=True,
        retries=3,
        retry_delay=timedelta(seconds=5),
        on_failure_callback=send_failure_email
    )
    def extract(params: dict = None):
        """
        #### Extract task
        """
        price_url = "https://mempool.space/api/v1/prices"
        hashrate_url = "https://mempool.space/api/v1/mining/hashrate/24h"

        async def get_data():
            async with aiohttp.ClientSession() as session:
                # get timestamp when sending requests
                spider_ts = int(time.time())
                request_price = spider_ts >= int(
                    Variable.get('next_request_price_time', 0))
                request_hashrate = spider_ts >= int(
                    Variable.get('next_request_hashrate_time', 0))

                # send async request
                if request_price:
                    next_request_price_time = spider_ts +\
                                              params["FetchPriceInterval"]
                    Variable.set('next_request_price_time',
                                 next_request_price_time)
                    check_task_interval(spider_ts,
                                        next_request_price_time,
                                        params["FetchPriceInterval"],
                                        Price.__name__)

                    price_task = asyncio.create_task(fetch(session,
                                                     price_url,
                                                     params["ApiAlarmThreshold"])
                                                     )

                if request_hashrate:
                    next_request_hashrate_time = spider_ts +\
                                                 params["FetchHashrateInterval"]
                    Variable.set('next_request_hashrate_time',
                                 next_request_hashrate_time)
                    check_task_interval(spider_ts,
                                        next_request_hashrate_time,
                                        params["FetchHashrateInterval"],
                                        Hashrate.__name__)
                    hashrate_task = asyncio.create_task(fetch(session,
                                                              hashrate_url,
                                                              params["ApiAlarmThreshold"]))

                price_data = None
                hashrate_data = None
                if request_price:
                    price_data = await price_task
                if request_hashrate:
                    hashrate_data = await hashrate_task

                return {
                    "spider_ts": spider_ts,
                    "price": price_data,
                    "hashrate": hashrate_data
                }

        return asyncio.get_event_loop().run_until_complete(get_data())

    @task(on_failure_callback=send_failure_email)
    def transform(input_data, params: dict = None):
        """
        #### Transform task
        Transform data from API
        """
        data = api_trans(input_data)
        try:
            mysql_trans(data,
                        MySqlHook(mysql_conn_id="mysql_mining").get_conn())
        except Exception as e:
            raise RuntimeError(f"Mysql data save failed."
                               f"The detail error info is:"
                               f"{e}\n{traceback.format_exc()}")

    @task(on_failure_callback=send_failure_email)
    def load(params: dict = None):
        """
        #### Load task
        Generate a dictionary with averages for BTC/USD price, Difficulty,
        and Hashrate in 5-minute intervals.
        """
        next_load_time = int(Variable.get('next_load_time', default_var=0))
        load_interval = params["LoadInterval"]
        spider_ts = int(time.time())

        if not next_load_time:
            Variable.set('next_load_time', spider_ts + load_interval)
        else:
            if spider_ts >= int(next_load_time):
                Variable.set('next_load_time', spider_ts + load_interval)
                logging.info("Loading starts.")

                try:
                    mysql_hook = MySqlHook(mysql_conn_id="mysql_mining").get_conn()
                    data = load_mysql_query({
                        "spider_ts": spider_ts,
                        "load_interval": load_interval
                    }, mysql_hook)
                    logging.info(f"load_mysql_query: {data}")
                    load_mysql_add(data, mysql_hook)

                except Exception as e:
                    raise RuntimeError(f"Load data failed."
                                       f"The detail error info is:"
                                       f" {e}\n{traceback.format_exc()}")

                return {
                    "spider_ts": spider_ts,
                    "price": data['avg_price'],
                    "hashrate": data['avg_hashrate'],
                    "difficulty": data['avg_difficulty']
                }

    request_data = extract()
    request_data >> transform(request_data) >> load()


bitcoin_mining_record()

if __name__ == "__main__":
    bitcoin_mining_record().test()
