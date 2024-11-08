import logging
from unittest.mock import Mock, call
from mining.data_model import Price, Hashrate
from mining.utils.transform import api_trans, mysql_trans


def test_api_trans_with_price_data():
    price_data = {
        "price_data": {
            "USD": 50000,
            "time": 1609459200
        },
        "spider_ts": 1609459200
    }

    expected = {
        "spider_ts": 1609459200,
        "price": Price(
            USD=50000,
            server_ts=1609459200
        )
    }

    result = api_trans(price_data, None)
    assert result == expected
    logging.info("test_api_trans_with_price_data passed")


def test_api_trans_with_hash_rate_data():
    hash_rate_data = {
        "hash_rate_data": {
            "currentHashrate": 100000000,
            "currentDifficulty": 15000000
        },
        "spider_ts": 1609459200
    }

    expected = {
        "spider_ts": 1609459200,
        "hashrate": Hashrate(
            hashrate="100000000",
            difficulty="15000000",
            server_ts=1609459200
        )
    }

    result = api_trans(None, hash_rate_data)
    assert result == expected
    logging.info("test_api_trans_with_hash_rate_data passed")


def test_api_trans_with_no_data():
    result = api_trans(None, None)
    assert result is None
    logging.info("test_api_trans_with_no_data passed")


def test_mysql_trans():
    model_data = {
        "spider_ts": 1609459200,
        "price": Price(
            USD=50000,
            server_ts=1609459200
        ),
        "hashrate": Hashrate(
            hashrate="100000000",
            difficulty="15000000",
            server_ts=1609459200
        )
    }

    mock_cursor = Mock()
    mock_conn = Mock(cursor=Mock(return_value=mock_cursor))

    mysql_trans(model_data, mock_conn)
    assert mock_cursor.execute.call_count == 2
    mock_cursor.execute.assert_has_calls([
        call(
            model_data["price"].SQL_INSERT,
            (*model_data["price"].sql_args, model_data["spider_ts"])
        ),
        call(
            model_data["hashrate"].SQL_INSERT,
            (*model_data["hashrate"].sql_args, model_data["spider_ts"])
        )
    ])
    mock_conn.commit.assert_called_once()
    logging.info("test_mysql_trans passed")
