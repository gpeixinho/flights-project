import datetime
import time
import configparser

from schedule import repeat, every, run_pending
from ingestion.decolar.ingestors import WebSearchIngestor
from ingestion.opensky.ingestors import AirportFlightsIngestor

# from opensky.writers import DataWriter
import ingestion.decolar.writers
import ingestion.opensky.writers
from ingestion.decolar.apis import WebSearchApi

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

source = "decolar"

if __name__ == "__main__":
    
    if source == "opensky":
        airport_flights_ingestor = AirportFlightsIngestor(
            username=username,
            password=password,
            writer=ingestion.opensky.writers.LocalWriter,
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

    if source == "decolar":
        websearch_ingestor = WebSearchIngestor(
            x_uow_token=x_uow_token,
            user_agent=user_agent,
            h_token=h_token,
            user_id=user_id,
            page_view_id=page_view_id,
            tracking_code=tracking_code,
            gui_version=gui_version,
            writer=ingestion.decolar.writers.LocalWriter,
            from_airports_iata=["GRU", "HND"],
            to_airports_iata=["LHR", "GRU"],
            departure_dates=[
                datetime.datetime(year=2022, month=10, day=25),
                datetime.datetime(year=2022, month=10, day=25),
            ],
            return_dates=[None, None],
            adults_list=[None, None],
            children_list=[None, None],
            infants_list=[None, None],
        )
        websearch_ingestor.ingest()
