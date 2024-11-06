from abc import ABC, abstractmethod
from dataclasses import dataclass


class DataModel(ABC):
    SQL_INSERT: str

    @abstractmethod
    def __eq__(self, other):
        ...

    @property
    @abstractmethod
    def __name__(self):
        ...

    @property
    @abstractmethod
    def sql_args(self):
        ...


@dataclass
class Price(DataModel):
    server_ts: int
    USD: int
    SQL_INSERT = "INSERT INTO price (USD, server_ts, spider_ts) " \
                 "VALUES (%s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s))"
    SQL_QUERY_AVG = "SELECT AVG(USD) AS USD FROM price WHERE server_ts " \
                    "BETWEEN FROM_UNIXTIME(%s) and FROM_UNIXTIME(%s)"

    def __eq__(self, other):
        return self.server_ts == other.server_ts and self.USD == other.USD

    def __str__(self):
        return f"server_ts: {self.server_ts}, USD: {self.USD}"

    @property
    def sql_args(self):
        """turn Price values into sql args"""
        return self.USD, self.server_ts

    @property
    def __name__(self):
        return "Price"


@dataclass
class Hashrate(DataModel):
    server_ts: int
    hashrate: str
    difficulty: str
    SQL_INSERT = "INSERT INTO hashrate (hashrate, difficulty, server_ts, spider_ts) " \
                 "VALUES (%s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s))"
    SQL_QUERY = "SELECT hashrate, difficulty FROM hashrate " \
                "WHERE server_ts BETWEEN FROM_UNIXTIME(%s) and FROM_UNIXTIME(%s)"

    @property
    def sql_args(self):
        return self.hashrate, self.difficulty, self.server_ts

    @property
    def __name__(self):
        return "Hashrate"

    def __str__(self):
        return f"hashrate: {self.hashrate}, " \
               f"difficulty: {self.difficulty}, " \
               f"server_ts: {self.server_ts}"

    def __eq__(self, other):
        return self.hashrate == other.hashrate and self.difficulty == other.difficulty
