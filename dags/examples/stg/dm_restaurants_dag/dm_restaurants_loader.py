from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import json
from datetime import datetime

class DmRestaurantsObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime
   
class DmRestaurantsDdsObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime

class DmRestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, restaurant_threshold: int, limit: int) -> List[DmRestaurantsObj]:
        with self._db.client().cursor(row_factory=class_row(DmRestaurantsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": restaurant_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DmRestaurantsDestRepository:

    def insert_restaurant(self, conn: Connection, restaurant: DmRestaurantsDdsObj) -> None:
        with conn.cursor() as cur:

            cur.execute(
                """
                    update dds.dm_restaurants
                    set active_to = %(active_from)s
                    where restaurant_id = %(restaurant_id)s
                    and active_to = '2099-12-31 00:00:00.000' """,
                {
                    "active_from": restaurant.active_from,
                    "restaurant_id": restaurant.restaurant_id
                }
            )
            cur.execute("""
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s);
                """,
                {
                     "restaurant_id": restaurant.restaurant_id,
                     "restaurant_name": restaurant.restaurant_name,
                     "active_from": restaurant.active_from, 
                     "active_to": '2099-12-31 00:00:00.000'
                },
            )


class DmRestaurantsLoader:
    WF_KEY = "example_restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmRestaurantsOriginRepository(pg_origin)
        self.stg = DmRestaurantsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Map DmRestaurantsObj to DmRestaurantsDdsObj
            def map_restaurant(restaurant: DmRestaurantsObj) -> DmRestaurantsDdsObj:
                object_value_dict = json.loads(restaurant.object_value)
                return DmRestaurantsDdsObj(
                    restaurant_id=restaurant.object_id,
                    restaurant_name=object_value_dict["name"],
                    active_from=restaurant.update_ts
            )

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for restaurant in load_queue:
                mapped_restaurant = map_restaurant(restaurant)
                self.stg.insert_restaurant(conn, mapped_restaurant)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

