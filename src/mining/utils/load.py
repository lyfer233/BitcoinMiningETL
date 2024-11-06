from MySQLdb.connections import Connection
from MySQLdb.cursors import Cursor
from decimal import Decimal

from mining.data_model import Hashrate, Price


def load_mysql_query(input_data: dict, mysql_hook: Connection) -> dict:
    time_range_start = input_data['spider_ts'] - input_data['load_interval']
    mysql_cursor: Cursor = mysql_hook.cursor()

    # Query price data for the last 5 minutes
    mysql_cursor.execute(Price.SQL_QUERY_AVG,
                         (time_range_start, input_data['spider_ts']))
    data = mysql_cursor.fetchone()
    # If the current price doesn't exist, try to get last 10 mins data
    if not data[0]:
        mysql_cursor.execute(Price.SQL_QUERY_AVG,
                             (time_range_start - input_data['load_interval'],
                              time_range_start)
                             )
        data = mysql_cursor.fetchone()
        if not data[0]:
            raise RuntimeError(
                "Mysql: no price data found in last 10 minutes.")

    avg_price = float(data[0])

    # Query hashrate and difficulty data for the last 5 minutes
    mysql_cursor.execute(Hashrate.SQL_QUERY,
                         (time_range_start, input_data['spider_ts']))
    data = mysql_cursor.fetchall()
    len_data = len(data)
    avg_hashrate = Decimal(sum(int(i[0]) for i in data) / len_data)
    avg_difficulty = Decimal(sum(float(i[1]) for i in data) / len_data)

    return {
        "spider_ts": input_data['spider_ts'],
        "avg_price": avg_price,
        "avg_hashrate": avg_hashrate,
        "avg_difficulty": avg_difficulty
    }


def load_mysql_add(input_data: dict, mysql_hook: Connection):
    mysql_cursor: Cursor = mysql_hook.cursor()
    # store result
    mysql_cursor.execute("INSERT INTO avg_info "
                         "(USD, hashrate, difficulty, spider_ts) "
                         "VALUES (%s, %s, %s, FROM_UNIXTIME(%s))",
                         (input_data['avg_price'],
                          f"{input_data['avg_hashrate']:.2f}",
                          f"{input_data['avg_difficulty']:.2f}",
                          input_data['spider_ts']))
    mysql_hook.commit()
