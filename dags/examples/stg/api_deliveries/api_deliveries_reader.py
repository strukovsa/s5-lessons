
from datetime import datetime, timedelta
from typing import Dict, List

import requests

def fetch_data_from_api(restaurant_id: str, from_ts: str, to_ts: str, sort_field: str, sort_direction: str, limit: int, offset: int, api_key: str, nickname: str, cohort_number: str):
    url = f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?restaurant_id={restaurant_id}&from={from_ts}&to={to_ts}"
    params = {
        "sort_field": sort_field,
        "sort_direction": sort_direction,
        "limit": limit,
        "offset": offset
    }
    headers = {
        "X-Nickname": nickname,
        "X-Cohort": cohort_number,
        "X-API-KEY": api_key
    }
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

class DeliveriesReader:
    def __init__(self, api_key: str, nickname: str, cohort_number: str) -> None:
        self.api_key = api_key
        self.nickname = nickname
        self.cohort_number = cohort_number

    def get_deliveries(self, restaurant_id: str, limit: int) -> List[Dict]:
        from_ts = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        to_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sort_field = "_id"
        sort_direction = "asc"
        offset = 0
        page_limit = 50

        result = []
        while True:
            data = fetch_data_from_api(restaurant_id, from_ts, to_ts, sort_field, sort_direction, page_limit, offset, self.api_key, self.nickname, self.cohort_number)
            result.extend(data)

            if len(data) < page_limit or len(result) >= limit:
                break

            offset += page_limit

        return result


