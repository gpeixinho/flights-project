from abc import ABC, abstractmethod
from typing import List
from wikipedia.scrapers import AirportCodeScraper


class DataIngestor(ABC):
    def __init__(self, writer) -> None:
        self.writer = writer

    @abstractmethod
    def ingest(self, **kwargs):
        pass


class AirportCodeIngestor(DataIngestor):
    def __init__(self, writer, letters: List):
        super().__init__(writer)
        self.letters = letters

    def ingest(self):
        for letter in self.letters:
            scraper = AirportCodeScraper(letter=letter)
            data = scraper.get_data()
            self.writer(path=f"{scraper.path}/letter={scraper.letter}").write(data)
