import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta


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
    time: int
    USD: int
    SQL_INSERT = "INSERT INTO price (time, USD, create_time) VALUES (FROM_UNIXTIME(%s), %s, FROM_UNIXTIME(%s))"
    SQL_QUERY_AVG = "SELECT AVG(USD) AS USD FROM price WHERE time BETWEEN FROM_UNIXTIME(%s) and FROM_UNIXTIME(%s)"

    def __eq__(self, other):
        return self.time == other.time and self.USD == other.USD

    def __str__(self):
        return f"time: {self.time}, USD: {self.USD}"

    @property
    def sql_args(self):
        """turn Price values into sql args"""
        return self.time, self.USD

    @property
    def __name__(self):
        return "Price"


@dataclass
class Hashrate(DataModel):
    hashrate: str
    difficulty: str
    SQL_INSERT = "INSERT INTO hashrate (hashrate, difficulty, create_time) VALUES (%s, %s, FROM_UNIXTIME(%s))"
    SQL_QUERY = "SELECT hashrate, difficulty FROM hashrate " \
                "WHERE create_time BETWEEN FROM_UNIXTIME(%s) and FROM_UNIXTIME(%s)"

    @property
    def sql_args(self):
        return self.hashrate, self.difficulty

    @property
    def __name__(self):
        return "Hashrate"

    def __str__(self):
        return f"hashrate: {self.hashrate}, difficulty: {self.difficulty}"

    def __eq__(self, other):
        return self.hashrate == other.hashrate and self.difficulty == other.difficulty
