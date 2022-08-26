import pandas as pd
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class WikipediaScraper(ABC):
    def __init__(self):
        self.base_endpoint = "https://en.wikipedia.org"

    @abstractmethod
    def _get_endpoint(self, **kwargs) -> str:
        pass

    def get_data(self, **kwargs) -> pd.DataFrame:
        endpoint = self._get_endpoint(**kwargs)
        response = pd.read_html(endpoint)
        logger.info(f"Getting data from endpoint: {endpoint}")
        return response

class AirportCodeScraper(WikipediaScraper):
    def __init__(self, letter: str):
        super().__init__()
        if len(letter) != 1:
            raise ValueError("letter attribute should be just one character")
        if not letter.isalpha():
            raise ValueError("letter should be an alphanumeric character")            
        self.letter = letter
        self.path = 'List_of_airports_by_IATA_airport_code'

    def _get_endpoint(self) -> str:
        endpoint = f"{self.base_endpoint}/wiki/{self.path}:_{self.letter}"
        return endpoint

