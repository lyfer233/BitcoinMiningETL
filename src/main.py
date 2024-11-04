import asyncio
import configparser
import logging
import time
from datetime import timedelta

import aiohttp
import pendulum
from MySQLdb import Connection
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.redis.hooks.redis import RedisHook
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.providers.mysql.hooks.mysql import MySqlHook
from redis import Redis
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
            from_email="local@mail.com",
            to="local@mail.com",
            subject="[Error] The dag {{ dag.dag_id }} failed",
            html_content="debug logs",
        )
    ],
    params={
        "RedisKeyExpiredTime": 360,
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
                raise Exception(f"Failed to fetch data from {url}, http status is {response.status}")

    @task(multiple_outputs=True)
    def extract():
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
                # send async request
                price_task = asyncio.create_task(fetch(session, price_url))
                hashrate_task = asyncio.create_task(fetch(session, hashrate_url))
                price_data = await price_task
                hashrate_data = await hashrate_task

                # convert to data model
                price = Price(**price_data)
                hashrate = Hashrate(str(hashrate_data["currentHashrate"]), str(hashrate_data["currentDifficulty"]))
                return {
                    "now": now,
                    "price": price,
                    "hashrate": hashrate
                }

        return asyncio.get_event_loop().run_until_complete(get_data())

    def update_db(item, now, redis, redis_key: str, key_expire_time: int, mysql, query_field: tuple | None = None):
        """
        Compare the data in the database with the data in the current item and update it.
        :return: None
        """
        mysql_cursor: Cursor = mysql.cursor()
        # check whether the item exists
        last_item = redis.get(redis_key)
        redis.expire(redis_key, key_expire_time)
        if not last_item:
            mysql_cursor.execute(item.SQL_QUERY, query_field)
            last_item_mysql = mysql_cursor.fetchone()
            # confirm the data is new
            if last_item_mysql:
                last_item = item.from_mysql_select(last_item_mysql)
                if last_item == item:
                    # redis data expired, add again
                    redis.set(redis_key, str(item), ex=key_expire_time)
                else:
                    # For hashrate, new value means an update.
                    if isinstance(item, Hashrate):
                        # FIXME 需使用redis锁
                        mysql_cursor.execute(item.SQL_EXIST, item.to_sql_args())
                        if mysql_cursor.fetchone():
                            raise Exception("exist")
                        mysql_cursor.execute(item.SQL_INSERT, (*item.to_sql_args(), now))
                        mysql.commit()
                    else:
                        raise Exception(f"Mysql: {item.__name__()} changed from '{last_item}' to '{item}'.")
            else:
                mysql_cursor.execute(item.SQL_INSERT, (*item.to_sql_args(), now))
                mysql.commit()
        else:
            last_item = last_item.decode("utf-8")
            if last_item != str(item):
                raise Exception(f"Redis: {item.__name__()} changed from '{last_item}' to '{item}'.")

    @task
    def transform(data, params: dict = None):
        """
        #### Transform task
        Transform data from API
        """
        now, price, hashrate = data["now"], data["price"], data["hashrate"]

        # init db conn
        redis_client: Redis = RedisHook(redis_conn_id="redis").get_conn()
        mysql_hook: Connection = MySqlHook(mysql_conn_id="mysql_mining").get_conn()

        update_db(price, now,
                  redis=redis_client,
                  redis_key=f"price::{price.time}",
                  key_expire_time=params["RedisKeyExpiredTime"],
                  mysql=mysql_hook,
                  query_field=(price.time,))
        update_db(hashrate, now,
                  redis=redis_client,
                  redis_key=f"hashrate::{hashrate}",
                  key_expire_time=params["RedisKeyExpiredTime"],
                  mysql=mysql_hook)

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

                mysql_hook: Connection = MySqlHook(mysql_conn_id="mysql_mining").get_conn()
                mysql_cursor: Cursor = mysql_hook.cursor()

                # Query price data for the last 5 minutes
                mysql_cursor.execute(Price.SQL_QUERY_AVG, (time_range_start, now))
                data = mysql_cursor.fetchone()
                avg_price = data[0]
                logging.info(f"price {data}")

                # Query hashrate and difficulty data for the last 5 minutes
                mysql_cursor.execute(Hashrate.SQL_QUERY_AVG, (time_range_start, now))
                data = mysql_cursor.fetchone()
                avg_hashrate, avg_difficulty = data
                logging.info(f"Hashrate {data}")

                # store result
                mysql_cursor.execute("""
                    INSERT INTO avg_info (USD, hashrate, difficulty, create_time) 
                    VALUES (%s, %s, %s, FROM_UNIXTIME(%s))
                """, (avg_price, avg_hashrate, avg_difficulty, now))
                mysql_hook.commit()

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
