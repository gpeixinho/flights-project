from abc import ABC, abstractmethod
from typing import List
from decolar.apis import WebSearchApi

class DataIngestor(ABC):
    def __init__(
        self,
        x_uow_token: str,
        user_agent: str,
        h_token: str,
        user_id: str,
        page_view_id: str,
        tracking_code: str,
        gui_version: str,
        writer,
    ) -> None:
        self.x_uow_token = x_uow_token
        self.user_agent = user_agent
        self.h_token = h_token
        self.user_id = user_id
        self.page_view_id = page_view_id
        self.tracking_code = tracking_code
        self.gui_version = gui_version
        self.writer = writer

    @abstractmethod
    def ingest(self, **kwargs):
        pass


class WebSearchIngestor(DataIngestor):
    def __init__(
        self,
        from_airports_iata: List,
        to_airports_iata: List,
        departure_dates: List,
        return_dates: List,
        adults_list: List,
        children_list: List,
        infants_list: List,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        lists = [
            from_airports_iata,
            to_airports_iata,
            departure_dates,
            return_dates,
            adults_list,
            children_list,
            infants_list,
        ]
        it = iter(lists)
        expected_len = len(next(it))
        if not all(len(l) == expected_len for l in it):
            raise ValueError("all parameters lists should have the same length")

        self.from_airports_iata = from_airports_iata
        self.to_airports_iata = to_airports_iata
        self.departure_dates = departure_dates
        self.return_dates = return_dates
        self.adults_list = adults_list
        self.children_list = children_list
        self.infants_list = infants_list

    def ingest(self):
        api = WebSearchApi(
            h_token=self.h_token,
            x_uow_token=self.x_uow_token,
            user_agent=self.user_agent,
            user_id=self.user_id,
            gui_version=self.gui_version,
            tracking_code=self.tracking_code,
        )
        for (
            from_airport_iata,
            to_airport_iata,
            departure_date,
            return_date,
            adults,
            children,
            infants,
        ) in zip(
            self.from_airports_iata,
            self.to_airports_iata,
            self.departure_dates,
            self.return_dates,
            self.adults_list,
            self.children_list,
            self.infants_list,
        ):
            optional_args = {
                "return_date": return_date,
                "adults": adults,
                "children": children,
                "infants": infants,
            }
            data = api.get_data(
                from_airport_iata=from_airport_iata,
                to_airport_iata=to_airport_iata,
                departure_date=departure_date,
                # Optional arguments will be set as None in a list if we intend to call the function without passing them
                **{
                    key: value
                    for key, value in optional_args.items()
                    if value is not None
                },
            )
            self.writer(api="websearch").write(data)
            page_count = data.get("pagination").get("pageCount")
            if page_count > 1:
                items_count = data.get("pagination").get("itemsCount")
                search_id = data.get("searchId")
                offset = 10
                while offset < items_count:
                    data = api.get_data(
                        from_airport_iata=from_airport_iata,
                        to_airport_iata=to_airport_iata,
                        departure_date=departure_date,
                        offset=offset,
                        search_id=search_id,
                        # Optional arguments will be set as None in a list if we intend to call the function without passing them
                        **{
                            key: value
                            for key, value in optional_args.items()
                            if value is not None
                        },
                    )
                    self.writer(api="websearch").write(data)
                    offset += 10
