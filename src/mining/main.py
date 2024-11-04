import asyncio
import logging
import time
from datetime import timedelta

import aiohttp
import pendulum
from MySQLdb import Connection
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.providers.mysql.hooks.mysql import MySqlHook
from MySQLdb.cursors import Cursor

from data_model import Price, Hashrate


@dag(
    dag_id="bitcoin_mining",
    schedule=timedelta(seconds=1),
    start_date=pendulum.datetime(2024, 11, 1),
    catchup=False,
    tags=["mining"],
    on_failure_callback=[
        send_smtp_notification(
            from_email="test@test.com",
            to="test@test.com",
            subject="[Error] The dag {{ dag.dag_id }} failed",
            html_content="debug logs",
        )
    ],
    params={
        "FetchPriceInterval": 5,
        "FetchHashrateInterval": 5,
        "LoadInterval": 300
    }
)
def bitcoin_mining_record():
    """
    """

    async def fetch(session, url):
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                raise RuntimeError(f"Failed to fetch data from {url}, http status is {response.status}")

    @task(multiple_outputs=True)
    def extract(params: dict = None):
        """
        #### Extract task
        Extract data from external API
        """
        price_url = "https://mempool.space/api/v1/prices"
        hashrate_url = "https://mempool.space/api/v1/mining/hashrate/24h"

        async def get_data():
            async with aiohttp.ClientSession() as session:
                # get timestamp when sending requests
                now = int(time.time())

                request_price = now - Variable.get('last_request_price_time', 0) >= params["FetchPriceInterval"]
                request_hashrate = now - Variable.get('last_request_hashrate_time', 0) >= params[
                    "FetchHashrateInterval"]

                # send async request
                if request_price:
                    Variable.get('last_request_price_time', now)
                    price_task = asyncio.create_task(fetch(session, price_url))
                if request_hashrate:
                    Variable.get('last_request_hashrate_time', now)
                    hashrate_task = asyncio.create_task(fetch(session, hashrate_url))

                if request_price:
                    price_data = await price_task
                if request_hashrate:
                    hashrate_data = await hashrate_task

                return {
                    "now": now,
                    "price": price_data,
                    "hashrate": hashrate_data
                }

        return asyncio.get_event_loop().run_until_complete(get_data())

    def api_trans(api_data: dict):
        """clean and convert to data model"""
        try:
            trans_data = {
                "now": api_data["now"],
                "price": Price(USD=api_data["price"]["USD"],
                               time=api_data["price"]["time"]),
                "hashrate": Hashrate(hashrate=str(api_data["hashrate"]["currentHashrate"]),
                                     difficulty=str(api_data["hashrate"]["currentDifficulty"]))
            }
            return trans_data
        except Exception as e:
            raise RuntimeError(f"Api data transform failed. The detail error info is as follow:\n{e}")

    def mysql_trans(model_data: dict):
        """save data"""
        now = model_data["now"]
        items = [model_data.get("price", None), model_data.get("hashrate", None)]

        # init db conn
        mysql_hook: Connection = MySqlHook(mysql_conn_id="mysql_mining").get_conn()
        mysql_cursor: Cursor = mysql_hook.cursor()
        for item in items:
            if item:
                mysql_cursor.execute(item.SQL_INSERT, (*item.sql_args, now))
                mysql_hook.commit()

    @task
    def transform(data, params: dict = None):
        """
        #### Transform task
        Transform data from API
        """
        data = api_trans(data)
        mysql_trans(data)

    # noinspection SqlNoDataSourceInspection
    @task
    def load(params: dict = None):
        """
        #### Load task
        Generate a dictionary with averages for BTC/USD price, Difficulty, and Hashrate in 5-minute intervals.
        """
        # judge whether the last load is too long ago
        last_load_time = Variable.get('last_load_time', default_var=None)
        load_interval = params["LoadInterval"]

        if last_load_time:
            if time.time() - int(last_load_time) > params["LoadInterval"]:
                logging.info("Loading starts.")

                now = int(time.time())
                logging.info(f"now {now}")
                time_range_start = now - load_interval

                try:
                    mysql_hook: Connection = MySqlHook(mysql_conn_id="mysql_mining").get_conn()
                    mysql_cursor: Cursor = mysql_hook.cursor()

                    # Query price data for the last 5 minutes
                    # FIXME no data available
                    mysql_cursor.execute(Price.SQL_QUERY_AVG, (time_range_start, now))
                    data = mysql_cursor.fetchone()
                    avg_price = data[0]
                    logging.info(f"price {data}")

                    # Query hashrate and difficulty data for the last 5 minutes
                    mysql_cursor.execute(Hashrate.SQL_QUERY, (time_range_start, now))
                    data = mysql_cursor.fetchall()
                    len_data = len(data)
                    avg_hashrate = sum(int(i[0]) for i in data) / len_data
                    avg_difficulty = sum(float(i[1]) for i in data) / len_data

                    # store result
                    mysql_cursor.execute("""
                                        INSERT INTO avg_info (USD, hashrate, difficulty, create_time) 
                                        VALUES (%s, %s, %s, FROM_UNIXTIME(%s))
                                    """, (avg_price, f"{avg_hashrate:.2f}", f"{avg_difficulty:.2f}", now))
                    mysql_hook.commit()
                except Exception as e:
                    raise RuntimeError(f"Load data failed. The detail error info is as follow:\n{e}")
                finally:
                    # update load time
                    Variable.set('last_load_time', now)

                return {
                    "time": now,
                    "price": avg_price,
                    "hashrate": avg_hashrate,
                    "difficulty": avg_difficulty
                }
        else:
            Variable.set('last_load_time', int(time.time()))

    request_data = extract()
    request_data >> transform(request_data) >> load()


bitcoin_mining_record()

if __name__ == "__main__":
    bitcoin_mining_record().test()
