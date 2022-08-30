from abc import ABC, abstractmethod
from typing import List, Union
import pandas as pd
import os


class DataTypeNotSupportedForIngestionException(Exception):
    def __init__(self, data):
        self.data = data
        self.message = f"Data type {type(data)} is not supported for ingestion"
        super().__init__(self.message)


class DataWriter(ABC):
    def __init__(self, path: str) -> None:
        self.path = path

    @property
    @abstractmethod
    def filename(self):
        pass

    @abstractmethod
    def write(self, **kwargs):
        pass


class LocalWriter(DataWriter):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.file_counter = 0

    @property
    def filename(self):
        return f"data/wikipedia/{self.path}/"

    def _write_file(self, data: pd.DataFrame) -> None:
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        data.to_csv(f"{self.filename}tb_{self.file_counter}.csv")
        self.file_counter += 1

    def write(self, data: Union[List, pd.DataFrame]):
        if isinstance(data, pd.DataFrame):
            self._write_file(data)
        elif isinstance(data, List):
            for element in data:
                self.write(element)
        else:
            raise DataTypeNotSupportedForIngestionException(data)
