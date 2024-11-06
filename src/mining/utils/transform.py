import traceback

from MySQLdb.connections import Connection
from MySQLdb.cursors import Cursor

from mining.data_model import Price, Hashrate


def api_trans(api_data: dict):
    """clean and convert to data model"""
    try:
        trans_data = {
            "spider_ts": api_data["spider_ts"],
            "price": Price(USD=api_data["price"]["USD"],
                           server_ts=api_data["price"]["time"])
            if api_data["price"] is not None else None,

            "hashrate": Hashrate(
                hashrate=str(api_data["hashrate"]["currentHashrate"]),
                difficulty=str(api_data["hashrate"]["currentDifficulty"]),
                server_ts=api_data["spider_ts"]
            )
            if api_data["hashrate"] is not None else None
        }
        return trans_data
    except Exception as e:
        raise RuntimeError(f"Api data transform failed."
                           f"The detail error info is:"
                           f" {e}\n{traceback.format_exc()}"
                           )


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
