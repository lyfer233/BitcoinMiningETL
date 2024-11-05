import asyncio
import logging
import time
import traceback
from datetime import timedelta

import aiohttp
import pendulum
from MySQLdb import Connection
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.providers.mysql.hooks.mysql import MySqlHook
from MySQLdb.cursors import Cursor

from mining.data_model import Price, Hashrate


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
        "ApiAlarmThreshold": 5,
        "FetchPriceInterval": 5,
        "FetchHashrateInterval": 5,
        "LoadInterval": 300
    }
)
def bitcoin_mining_record():
    """
    """
    async def fetch(session, url, api_alarm_threshold):
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                api_error_count = int(Variable.get('api_error_count', 0))
                if api_error_count >= api_alarm_threshold:
                    Variable.set('api_error_count', 0)
                    raise RuntimeError(f"Failed to fetch data from {url}, "
                                       f"exceeding threshold, HTTP status code is {response.status}")
                else:
                    Variable.set('api_error_count', api_error_count + 1)

    def check_task_interval(now, next_time, interval, task_name):
        if now - next_time > interval:
            raise RuntimeError(f"{task_name} interval is too short to handle task.")

    @task(multiple_outputs=True)
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
                request_price = spider_ts >= int(Variable.get('next_request_price_time', 0))
                request_hashrate = spider_ts >= int(Variable.get('next_request_hashrate_time', 0))

                # send async request
                if request_price:
                    next_request_price_time = spider_ts + params["FetchPriceInterval"]
                    Variable.set('next_request_price_time', next_request_price_time)
                    check_task_interval(spider_ts, next_request_price_time, params["FetchPriceInterval"], Price.__name__)
                    price_task = asyncio.create_task(fetch(session, price_url, params["ApiAlarmThreshold"]))

                if request_hashrate:
                    next_request_hashrate_time = spider_ts + params["FetchHashrateInterval"]
                    Variable.set('next_request_hashrate_time', next_request_hashrate_time)
                    check_task_interval(spider_ts, next_request_hashrate_time, params["FetchHashrateInterval"], Hashrate.__name__)
                    hashrate_task = asyncio.create_task(fetch(session, hashrate_url, params["ApiAlarmThreshold"]))

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

    def api_trans(api_data: dict):
        """clean and convert to data model"""
        try:
            trans_data = {
                "spider_ts": api_data["spider_ts"],
                "price": Price(USD=api_data["price"]["USD"],
                               server_ts=api_data["price"]["time"]) if api_data["price"] is not None else None,
                "hashrate": Hashrate(hashrate=str(api_data["hashrate"]["currentHashrate"]),
                                     difficulty=str(api_data["hashrate"]["currentDifficulty"]),
                                     server_ts=api_data["spider_ts"]) if api_data["hashrate"] is not None else None
            }
            return trans_data
        except Exception as e:
            raise RuntimeError(f"Api data transform failed. The detail error info is: {e}\n{traceback.format_exc()}")

    def mysql_trans(model_data: dict, mysql_hook: Connection):
        """save data"""
        spider_ts = model_data["spider_ts"]
        items = [model_data.get("price", None), model_data.get("hashrate", None)]

        # init db conn
        mysql_cursor: Cursor = mysql_hook.cursor()
        for item in items:
            if item:
                mysql_cursor.execute(item.SQL_INSERT, (*item.sql_args, spider_ts))
                mysql_hook.commit()

    @task
    def transform(data, params: dict = None):
        """
        #### Transform task
        Transform data from API
        """
        data = api_trans(data)
        try:
            mysql_trans(data, MySqlHook(mysql_conn_id="mysql_mining").get_conn())
        except Exception as e:
            raise RuntimeError(f"Mysql data save failed. The detail error info is: {e}\n{traceback.format_exc()}")

    @task
    def load(params: dict = None):
        """
        #### Load task
        Generate a dictionary with averages for BTC/USD price, Difficulty, and Hashrate in 5-minute intervals.
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

                time_range_start = spider_ts - load_interval

                try:
                    mysql_hook: Connection = MySqlHook(mysql_conn_id="mysql_mining").get_conn()
                    mysql_cursor: Cursor = mysql_hook.cursor()

                    # Query price data for the last 5 minutes
                    mysql_cursor.execute(Price.SQL_QUERY_AVG, (time_range_start, spider_ts))
                    data = mysql_cursor.fetchone()
                    # If the current price doesn't exist, try to get last 10 mins data
                    if not data:
                        mysql_cursor.execute(Price.SQL_QUERY_AVG, (time_range_start - load_interval, time_range_start))
                        data = mysql_cursor.fetchone()
                        if not data:
                            raise RuntimeError("Mysql no data found in last 10 minutes.")

                    avg_price = data[0]
                    logging.info(f"price {data}")

                    # Query hashrate and difficulty data for the last 5 minutes
                    mysql_cursor.execute(Hashrate.SQL_QUERY, (time_range_start, spider_ts))
                    data = mysql_cursor.fetchall()
                    len_data = len(data)
                    avg_hashrate = sum(int(i[0]) for i in data) / len_data
                    avg_difficulty = sum(float(i[1]) for i in data) / len_data

                    # store result
                    mysql_cursor.execute("""
                                        INSERT INTO avg_info (USD, hashrate, difficulty, spider_ts) 
                                        VALUES (%s, %s, %s, FROM_UNIXTIME(%s))
                                    """, (avg_price, f"{avg_hashrate:.2f}", f"{avg_difficulty:.2f}", spider_ts))
                    mysql_hook.commit()
                except Exception as e:
                    raise RuntimeError(f"Load data failed. The detail error info is: {e}\n{traceback.format_exc()}")

                return {
                    "spider_ts": spider_ts,
                    "price": avg_price,
                    "hashrate": avg_hashrate,
                    "difficulty": avg_difficulty
                }

    request_data = extract()
    request_data >> transform(request_data) >> load()


bitcoin_mining_record()

if __name__ == "__main__":
    bitcoin_mining_record().test()
