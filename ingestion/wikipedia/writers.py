from abc import ABC, abstractmethod
from typing import List, Union
import pandas as pd
import os
import boto3
from tempfile import NamedTemporaryFile
import datetime


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
        self._file_counter = 0

    @property
    def filename(self):
        return f"data/wikipedia/{self.path}/tb_{self._file_counter}.csv"

    def _write_file(self, data: pd.DataFrame) -> None:
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        data.to_csv(self.filename, index=False, encoding="utf-8")
        self._file_counter += 1

    def write(self, data: Union[List, pd.DataFrame]):
        if isinstance(data, pd.DataFrame):
            self._write_file(data)
        elif isinstance(data, List):
            for element in data:
                self.write(element)
        else:
            raise DataTypeNotSupportedForIngestionException(data)


class S3Writer(DataWriter):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.tempfile = NamedTemporaryFile(suffix=".csv")
        self.client = boto3.client("s3")
        self.bucket = "flights-data-lake-raw"
        self._file_counter = 0

    @property
    def filename(self):
        return f"wikipedia/{self.path}/extracted_at={datetime.datetime.now().date()}/tb_{self._file_counter}.csv"

    def _write_file_to_s3(self, file) -> None:
        file.seek(0)
        self.client.put_object(
            Body=file, Bucket=self.bucket, Key=self.filename
        )

    def write(self, data: Union[List, pd.DataFrame]) -> None:
        if isinstance(data, pd.DataFrame):
            tempfile=NamedTemporaryFile(suffix=".csv")
            data.to_csv(tempfile, encoding="utf-8", index=False)
            self._write_file_to_s3(tempfile)
            self._file_counter += 1    
        elif isinstance(data, List):
            for element in data:
                self.write(element)
        else:
            raise DataTypeNotSupportedForIngestionException(data)     
