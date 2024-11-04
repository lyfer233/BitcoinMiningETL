import logging
from dataclasses import dataclass
from datetime import timedelta


@dataclass
class Price:
    time: int
    AUD: int
    CAD: int
    CHF: int
    EUR: int
    GBP: int
    JPY: int
    USD: int
    SQL_INSERT = "INSERT INTO price (time, AUD, CAD, CHF, EUR, GBP, JPY, USD, create_time) " \
                 "VALUES (FROM_UNIXTIME(%s), %s, %s, %s, %s, %s, %s, %s, FROM_UNIXTIME(%s))"
    SQL_QUERY = "SELECT time, AUD, CAD, CHF, EUR, GBP, JPY, USD FROM price WHERE time = FROM_UNIXTIME(%s)"
    SQL_QUERY_AVG = "SELECT AVG(USD) AS USD FROM price WHERE time BETWEEN FROM_UNIXTIME(%s) and FROM_UNIXTIME(%s)"

    def __eq__(self, other):
        return self.time == other.time and \
            self.AUD == other.AUD and \
            self.CAD == other.CAD and \
            self.CHF == other.CHF and \
            self.EUR == other.EUR and \
            self.GBP == other.GBP and \
            self.JPY == other.JPY and \
            self.USD == other.USD

    def __str__(self):
        return f"{self.USD}"

    def to_sql_args(self):
        """turn Price values into sql args"""
        return self.time, self.AUD, self.CAD, self.CHF, self.EUR, self.GBP, self.JPY, self.USD

    @staticmethod
    def from_mysql_select(args: tuple):
        """Turn mysql select result into Price"""
        # TimeZone transform: CST to UTC
        return Price(int((args[0] - timedelta(hours=8)).timestamp()), *args[1:8])

    @staticmethod
    def __name__():
        return "Price"


@dataclass
class Hashrate:
    hashrate: str
    difficulty: str
    SQL_INSERT = "INSERT INTO hashrate (hashrate, difficulty, create_time) VALUES (%s, %s, FROM_UNIXTIME(%s))"
    SQL_QUERY = "SELECT hashrate, difficulty FROM hashrate ORDER BY id LIMIT 1"
    SQL_EXIST = "SELECT 1 FROM hashrate WHERE hashrate=%s AND difficulty=%s"
    SQL_QUERY_AVG = "SELECT AVG(hashrate) AS avg_hashrate, AVG(difficulty) AS avg_difficulty " \
                    "FROM hashrate WHERE create_time BETWEEN FROM_UNIXTIME(%s) and FROM_UNIXTIME(%s)"

    def to_sql_args(self):
        return self.hashrate, self.difficulty

    @staticmethod
    def __name__():
        return "Hashrate"

    def __str__(self):
        return f"Hashrate: {self.hashrate}, Difficulty: {self.difficulty}"

    def __eq__(self, other):
        return self.hashrate == other.hashrate and self.difficulty == other.difficulty

    @staticmethod
    def from_mysql_select(args: tuple):
        """Turn mysql select result into Price"""
        return Hashrate(*args)