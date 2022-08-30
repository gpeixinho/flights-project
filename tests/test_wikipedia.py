import pytest
from ingestion.wikipedia.scrapers import AirportCodeScraper


class TestAirportCodeScraper:
    @pytest.mark.parametrize(
        "letter, expected",
        [
            ("A", "A"),
            ("Z", "Z"),
            ("a", "A"),
            ("z", "Z"),
        ],
    )
    def test_constructor_ok(self, letter, expected):
        actual = AirportCodeScraper(letter=letter).letter
        assert actual == expected

    @pytest.mark.parametrize(
        "letter",
        [("Aa"), ("Test")],
    )
    def test_constructor_exception_char_count(self, letter):
        with pytest.raises(
            ValueError,
            match="parameter 'letter' attribute should be just one character",
        ):
            scraper = AirportCodeScraper(letter=letter)

    @pytest.mark.parametrize(
        "letter",
        [
            ("-"),
            ("0"),
            ("!"),
            ("9"),
        ],
    )
    def test_constructor_exception_alpha_char(self, letter):
        with pytest.raises(
            ValueError, match="parameter 'letter' should be an alphanumeric character"
        ):
            scraper = AirportCodeScraper(letter=letter)

    @pytest.mark.parametrize(
        "letter, expected",
        [
            (
                "A",
                "https://en.wikipedia.org/wiki/List_of_airports_by_IATA_airport_code:_A",
            ),
            (
                "Z",
                "https://en.wikipedia.org/wiki/List_of_airports_by_IATA_airport_code:_Z",
            ),
            (
                "a",
                "https://en.wikipedia.org/wiki/List_of_airports_by_IATA_airport_code:_A",
            ),
            (
                "z",
                "https://en.wikipedia.org/wiki/List_of_airports_by_IATA_airport_code:_Z",
            ),
        ],
    )
    def test_get_endpoint(self, letter, expected):
        actual = AirportCodeScraper(letter=letter)._get_endpoint()
        assert actual == expected
