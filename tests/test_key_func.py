import time
import unittest
from unittest.mock import MagicMock, patch
from decimal import Decimal
from mining.data_model import Price, Hashrate
from mining.utils.extract import check_task_interval
from mining.utils.load import load_mysql_query, load_mysql_add
from mining.utils.transform import api_trans, mysql_trans


class TestBitcoinMining(unittest.TestCase):

    def test_check_task_interval(self):
        # normal condition
        now = int(time.time())
        interval = 5
        next_time = now + interval + 20

        task_name = "TestTask"
        try:
            check_task_interval(now, next_time, interval, task_name)
        except RuntimeError:
            self.fail("check_task_interval raised RuntimeError unexpectedly!")

        # overtime condition
        next_time = now - interval - 10
        with self.assertRaises(RuntimeError) as context:
            check_task_interval(now, next_time, interval, task_name)
        self.assertIn("interval is too short", str(context.exception))

    def test_api_trans(self):
        # API数据转换成功
        api_data = {
            "spider_ts": 1730859758,
            "price": {"USD": 71337, "time": 1730859303},
            "hashrate": {
                "currentHashrate": 713570774024820500000,
                "currentDifficulty": 101646843652785.2
            }
        }
        result = api_trans(api_data)
        self.assertEqual(result['spider_ts'], 1730859758)

        self.assertIsInstance(result['price'], Price)
        self.assertEqual(result['price'],
                         Price(USD=71337, server_ts=1730859303))

        self.assertIsInstance(result['hashrate'], Hashrate)
        self.assertEqual(result['hashrate'], Hashrate(
            hashrate='713570774024820500000',
            difficulty='101646843652785.2',
            server_ts=1730859758
        ))

        # API data convert fail
        api_data = {
            "spider_ts": 1730859758,
            "price": None,
            "hashrate": None
        }
        result = api_trans(api_data)
        self.assertEqual(result['spider_ts'], 1730859758)
        self.assertIsNone(result['price'])
        self.assertIsNone(result['hashrate'])

    @patch('MySQLdb.Connection')
    def test_mysql_trans(self, mock_mysql_conn):
        # mock data model
        model_data = {
            "spider_ts": 1730859758,
            "price": Price(USD=71337, server_ts=1730859303),
            "hashrate": Hashrate(
                hashrate=713570774024820500000,
                difficulty=101646843652785.2,
                server_ts=1730859303
            )
        }

        # mock mysql cursor
        mock_cursor = MagicMock()
        mock_mysql_conn.cursor.return_value = mock_cursor

        mysql_trans(model_data, mock_mysql_conn)

        # check result
        self.assertEqual(mock_cursor.execute.call_count, 2)
        mock_mysql_conn.commit.assert_called_once()

    @patch('MySQLdb.Connection')
    def test_load_mysql_query(self, mock_mysql_conn):
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (71985.4,)
        mock_cursor.fetchall.return_value = [
            (713570774024820500000, 101646843652785.2),
            (713570774024820500000, 101646843652785.2),
            (713570774024820500000, 101646843652785.2),
            (713570774024820500000, 101646843652785.2)
        ]
        mock_mysql_conn.cursor.return_value = mock_cursor

        input_data = {
            'spider_ts': 1730860063,
            'load_interval': 300
        }

        result = load_mysql_query(input_data, mock_mysql_conn)

        self.assertEqual(result['avg_price'], Decimal(71985.4))
        self.assertEqual(result['avg_hashrate'],
                         Decimal(713570774024820490240))
        self.assertEqual(result['avg_difficulty'], Decimal(101646843652785.2))

    @patch('MySQLdb.Connection')
    def test_load_mysql_add(self, mock_mysql_conn):
        input_data = {
            "spider_ts": 1730860063,
            "avg_price": Decimal(71985.4),
            "avg_hashrate": Decimal(713570774024820500000),
            "avg_difficulty": Decimal(101646843652785.20)
        }

        mock_cursor = MagicMock()
        mock_mysql_conn.cursor.return_value = mock_cursor

        load_mysql_add(input_data, mock_mysql_conn)

        mock_cursor.execute.assert_called_once_with(
            "INSERT INTO avg_info (USD, hashrate, difficulty, spider_ts) "
            "VALUES (%s, %s, %s, FROM_UNIXTIME(%s))",
            (
                71985.4,
                "713570774024820500000.00",
                "101646843652785.20",
                1730860063
             )
        )
        mock_mysql_conn.commit.assert_called_once()


if __name__ == '__main__':
    unittest.main()
