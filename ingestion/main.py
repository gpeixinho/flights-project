import datetime
import time
import configparser
import argparse

from schedule import repeat, every, run_pending
from decolar.ingestors import WebSearchIngestor
from opensky.ingestors import AirportFlightsIngestor
from opensky.ingestors import AllFlightsIngestor
from wikipedia.ingestors import AirportCodeIngestor

# from opensky.writers import DataWriter
import decolar.writers
import opensky.writers
import wikipedia.writers

from decolar.apis import WebSearchApi
from wikipedia.scrapers import AirportCodeScraper

config = configparser.ConfigParser()
config.read("configs")

username = config["OPENSKY"]["USERNAME"]
password = config["OPENSKY"]["PASSWORD"]

x_uow_token = config["DECOLAR"]["x_uow"]
user_agent = config["DECOLAR"]["user_agent"]
h_token = config["DECOLAR"]["h_token"]
user_id = config["DECOLAR"]["user_id"]
page_view_id = config["DECOLAR"]["page_view_id"]
tracking_code = config["DECOLAR"]["tracking_code"]
gui_version = config["DECOLAR"]["gui_version"]

def _parse_arguments():
    """Parse arguments provided by spark-submit command"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    return parser.parse_args()



if __name__ == "__main__":

    args = _parse_arguments()
    source = args.source

    if source == "opensky":
        airport_flights_ingestor = AirportFlightsIngestor(
            username=username,
            password=password,
            writer=opensky.writers.LocalWriter,
            airports=["KTPA", "KCLT"],
            types=["arrival", "departure"],
            default_start_date=datetime.datetime(2022, 6, 21),
        )

        @repeat(every(1).seconds)
        def job():
            airport_flights_ingestor.ingest()

        while True:
            run_pending()
            time.sleep(0.5)

    if source == "opensky_all":
        all_flights_ingestor = AllFlightsIngestor(
            username=username,
            password=password,
            writer=opensky.writers.S3Writer,
            default_start_date=datetime.datetime(2022, 6, 21),
        )
        @repeat(every(1).seconds)
        def job():
            all_flights_ingestor.ingest()

        while True:
            run_pending()
            time.sleep(0.5)

    if source == "decolar":
        websearch_ingestor = WebSearchIngestor(
            x_uow_token=x_uow_token,
            user_agent=user_agent,
            h_token=h_token,
            user_id=user_id,
            page_view_id=page_view_id,
            tracking_code=tracking_code,
            gui_version=gui_version,
            # writer=ingestion.decolar.writers.LocalWriter,
            writer=decolar.writers.S3Writer,
            from_airports_iata=["GRU", "GRU", "GRU", "GRU", "GRU"],
            to_airports_iata=["LHR", "LHR", "LHR", "LHR", "LHR"],
            departure_dates=[
                datetime.datetime(year=2022, month=11, day=30),
                datetime.datetime(year=2022, month=12, day=1),
                datetime.datetime(year=2022, month=12, day=2),
                datetime.datetime(year=2022, month=12, day=3),
                datetime.datetime(year=2022, month=12, day=4),
            ],
            return_dates=[
                datetime.datetime(year=2023, month=1, day=5),
                datetime.datetime(year=2023, month=1, day=6),
                datetime.datetime(year=2023, month=1, day=7),
                datetime.datetime(year=2023, month=1, day=8),
                datetime.datetime(year=2023, month=1, day=9),
            ],
            adults_list=[None] * 5,
            children_list=[None] * 5,
            infants_list=[None] * 5,
        )
        websearch_ingestor.ingest()

    if source == "wikipedia":
        letters = [
            "A",
            "B",
            "C",
            "D",
            "E",
            "F",
            "G",
            "H",
            "I",
            "J",
            "K",
            "L",
            "M",
            "N",
            "O",
            "P",
            "Q",
            "R",
            "S",
            "T",
            "U",
            "V",
            "W",
            "X",
            "Y",
            "Z",
        ]
        airport_code_ingestor = AirportCodeIngestor(
            letters=letters, writer=wikipedia.writers.LocalWriter
        )
        airport_code_ingestor.ingest()
