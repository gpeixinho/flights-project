import datetime
from abc import ABC, abstractmethod
from typing import List
from opensky.apis import AirportFlightsApi

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class DataIngestor(ABC):
    def __init__(
        self,
        username: str,
        password: str,
        writer,
        airports: List[str],
        types: List[str],
        default_start_date: datetime.date,
    ) -> None:
        self.username = username
        self.password = password
        self.default_start_date = default_start_date
        self.writer = writer
        self.airports = airports
        self.types = types
        self._checkpoint = self._load_checkpoint()

    @property
    def _checkpoint_filename(self) -> str:
        return f"{self.__class__.__name__}.checkpoint"

    def _write_checkpoint(self):
        with open(self._checkpoint_filename, "w") as f:
            f.write(f"{self._checkpoint}")

    def _load_checkpoint(self) -> datetime.date:
        try:
            with open(self._checkpoint_filename, "r") as f:
                checkpoint = datetime.datetime.strptime(f.read(), "%Y-%m-%d %H:%M:%S")
                logger.info(f"Found checkpoint {checkpoint}")
                return checkpoint
        except FileNotFoundError:
            logger.info(
                f"Initializing checkpoint with default start date {self.default_start_date}"
            )
            return self.default_start_date

    def _update_checkpoint(self, value):
        logger.info(f"Updating checkpoint to {value}")
        self._checkpoint = value
        self._write_checkpoint()

    @abstractmethod
    def ingest(self) -> None:
        pass


class AirportFlightsIngestor(DataIngestor):
    def ingest(self) -> None:
        date = self._load_checkpoint()
        if date < datetime.datetime.now():
            for airport in self.airports:
                begin = datetime.datetime(date.year, date.month, date.day)
                end = begin + datetime.timedelta(days=7)
                api = AirportFlightsApi(
                    username=self.username, password=self.password, airport=airport
                )
                for type in self.types:
                    data = api.get_data(begin=begin, end=end, type=type)
                    self.writer(airport=airport, api="flights", type=type).write(data)
            self._update_checkpoint(end + datetime.timedelta(seconds=1))
