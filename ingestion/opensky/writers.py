import datetime
import json
import os
from typing import List, Union
import logging 
from abc import ABC, abstractmethod, abstractproperty


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class DataTypeNotSupportedForIngestionException(Exception):
    def __init__(self, data):
        self.data = data
        self.message = f"Data type {type(data)} is not supported for ingestion"
        super().__init__(self.message)

class DataWriter(ABC):
    def __init__(self, airport: str, api: str, type: str) -> None:
        self.api = api
        self.airport = airport
        self.type = type

    @property
    @abstractmethod
    def filename(self):
        pass
            
    @abstractmethod
    def write(self, **kwargs):
        pass

class LocalWriter(DataWriter):

    @property
    def filename(self):
        return f"data/opensky/{self.api}/{self.airport}/{self.type}/{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"

    def _write_row(self, row: str) -> None:
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        with open(self.filename, "a") as f:
            f.write(row)

    def write(self, data: Union[List, dict]):
        if isinstance(data, dict):
            self._write_row(json.dumps(data) + "\n")
        elif isinstance(data, List):
            for element in data:
                self.write(element)
        else:
            raise DataTypeNotSupportedForIngestionException(data)

class S3Writer(DataWriter):
    #TODO: implement writer to S3 adopting data lake best practices
    pass