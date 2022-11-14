# Flight Data Ingestion and Analysis Project
[![en](https://img.shields.io/badge/lang-en-red.svg)](https://github.com/gpeixinho/flights-project/blob/master/README.md)
[![pt-br](https://img.shields.io/badge/lang-pt--br-green.svg)](https://github.com/gpeixinho/flights-project/blob/master/README.pt-br.md)

:warning: Under Development! :warning:	

Structuring a data lake containing flight offers data gathered automatically from travel agency websites and also open historical flight data for analysis of flight offers that have been canceled by airlines (no longer exist).

I have recently heard of cases of customers who bought tickets online from travel agencies, and when they were about to check-in they noticed that the flight simply did not exist, it may have existed before, but it's been sometime it is not performed by the airline. At the time I suspected this was a data integration issue and decided to build this to verify that the flights being offered happend in the past weeks or months.

## Goals

- Get hands-on learning with data engineering projects and technologies
- Build a data lake containing data from flights
- Analysing flights from offers that have not been performed lately

## Data sources and Diagram

- Travel angency API for flight offers
- Opensky network API for historical flights (https://opensky-network.org/)
- Wikipedia page for airport codes in different standards (https://en.wikipedia.org/wiki/Lists_of_airports_by_IATA_and_ICAO_code)


![Pipeline Diagram](imgs/diagram_partial_20221006.png)
