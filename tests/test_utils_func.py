import time
import unittest
from unittest.mock import MagicMock, patch
from decimal import Decimal
from mining.data_model import Price, Hashrate
from mining.utils.extract import fetch_data_from_api
from mining.utils.load import load_mysql_query, load_mysql_add
from mining.utils.transform import api_trans, mysql_trans


class TestBitcoinMining(unittest.TestCase):

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
