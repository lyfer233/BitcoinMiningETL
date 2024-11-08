import traceback
import logging

from MySQLdb.connections import Connection
from MySQLdb.cursors import Cursor

from mining.data_model import Price, Hashrate


def api_trans(price_data, hash_rate_data):
    """clean and convert to data model"""
    transformed_data = {}
    if price_data:
        transformed_data["spider_ts"] = price_data.get("spider_ts")
        transformed_data["price"] = Price(
            USD = price_data["price_data"]["USD"],
            server_ts = price_data["price_data"]["time"]
        )
        logging.info(f"Price data added to transformed_data: {transformed_data}")

    elif hash_rate_data:
        transformed_data["spider_ts"] = hash_rate_data.get("spider_ts")
        transformed_data["hashrate"] = Hashrate(
            hashrate=str(hash_rate_data["hash_rate_data"]["currentHashrate"]),
            difficulty=str(hash_rate_data["hash_rate_data"]["currentDifficulty"]),
            server_ts=hash_rate_data.get("spider_ts")
        )
        logging.info(f"hash rate data added to transformed_data: {transformed_data}")
    else:
        logging.info("transform task skipped aas no data was available.")
        return None
    return transformed_data

def mysql_trans(model_data: dict, mysql_hook: Connection):
    """save data"""
    if model_data is None:
        return
    spider_ts = model_data["spider_ts"]
    items = [model_data.get("price", None), model_data.get("hashrate", None)]

    # init db conn
    mysql_cursor: Cursor = mysql_hook.cursor()
    for item in items:
        if item:
            mysql_cursor.execute(item.SQL_INSERT, (*item.sql_args, spider_ts))
    mysql_hook.commit()
