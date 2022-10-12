import datetime
import os
import json
import boto3

from typing import Union, List
from abc import ABC, abstractmethod
from tempfile import NamedTemporaryFile


class DataTypeNotSupportedForIngestionException(Exception):
    def __init__(self, data):
        self.data = data
        self.message = f"Data type {type(data)} is not supported for ingestion"
        super().__init__(self.message)


class DataWriter(ABC):
    def __init__(self, api: str) -> None:
        self.api = api

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
        return f"data/decolar/{self.api}/{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"

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
    def __init__(self, api: str) -> None:
        super().__init__(api)
        self.tempfile = NamedTemporaryFile()
        self.client = boto3.client("s3")
        self.bucket = "flights-data-lake-raw"

    @property
    def filename(self) -> str:
        return f"decolar/{self.api}/extracted_at={datetime.datetime.now().date()}/{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"

    def _write_row(self, row: str) -> None:
        self.tempfile.write(row)

    def _write_file_to_s3(self) -> None:
        self.tempfile.seek(0)
        self.client.put_object(
            Body=self.tempfile, Bucket=self.bucket, Key=self.filename
        )

    def _write_to_file(self, data: Union[List, dict]) -> None:
        if isinstance(data, dict):
            self._write_row(f"{json.dumps(data)}\n".encode("utf-8"))
        elif isinstance(data, List):
            for element in data:
                self.write(element)
        else:
            raise DataTypeNotSupportedForIngestionException(data)
        
    def write(self, data: Union[List, dict]) -> None:
        self._write_to_file(data)
        self._write_file_to_s3()
