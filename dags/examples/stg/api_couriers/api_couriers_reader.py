
from typing import Dict, List

import requests

def fetch_data_from_api(sort_field: str, sort_direction: str, limit: int, offset: int, api_key: str, nickname: str, cohort_number: str):
    url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants"
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



class CouriersReader:
    def __init__(self, api_key: str, nickname: str, cohort_number: str) -> None:
        self.api_key = api_key
        self.nickname = nickname
        self.cohort_number = cohort_number

    def get_couriers(self, _, limit: int) -> List[Dict]:
        sort_field = "_id"
        sort_direction = "asc"
        offset = 0
        page_limit = 50

        result = []
        while True:
            data = fetch_data_from_api(sort_field, sort_direction, page_limit, offset, self.api_key, self.nickname, self.cohort_number)
            result.extend(data["items"])

            if len(data["items"]) < page_limit or len(result) >= limit:
                break

            offset += page_limit

        return result

