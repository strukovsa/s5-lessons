from logging import Logger
from typing import List, Optional

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import json
from datetime import datetime

class DmProductsObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime
   
class DmProductsDdsObj(BaseModel):
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int

class DmProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, product_threshold: int, limit: int) -> List[DmProductsObj]:
        with self._db.client().cursor(row_factory=class_row(DmProductsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs
    
    def get_restaurant_id(self, restaurant_id: str) -> Optional[int]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s
                    AND active_to = '2099-12-31 00:00:00.000';
                """,
                {"restaurant_id": restaurant_id},
            )
            result = cur.fetchone()
            if result:
                return result[0]
        return None


class DmProductsDestRepository:

    def insert_product(self, conn: Connection, product: DmProductsDdsObj) -> None:
        with conn.cursor() as cur:

            cur.execute(
                """
                    update dds.dm_products
                    set active_to = %(active_from)s
                    where product_id = %(product_id)s
                    and active_to = '2099-12-31 00:00:00.000' """,
                {
                    "active_from": product.active_from,
                    "restaurant_id": product.product_id
                }
            )
            cur.execute("""
                    INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s);
                """,
                {
                     "product_id": product.product_id,
                     "product_name": product.product_name,
                     "product_price": product.product_price,
                     "active_from": product.active_from, 
                     "active_to": '2099-12-31 00:00:00.000',
                     "restaurant_id": product.restaurant_id
                },
            )


class DmProductsLoader:
    WF_KEY = "example_products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmProductsOriginRepository(pg_origin)
        self.stg = DmProductsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_products(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Map DmProductsObj to DmProductsDdsObj
            def map_product(product: DmProductsObj) -> DmProductsDdsObj:
                object_value_dict = json.loads(product.object_value)
                restaurant_id = object_value_dict["restaurant"]["id"]
                correct_restaurant_id = self.origin.get_restaurant_id(restaurant_id)
                if correct_restaurant_id is None:
                    self.log.warning(f"Could not find a matching restaurant_id for {restaurant_id}. Skipping this product.")
                    return None

                return DmProductsDdsObj(
                product_id=object_value_dict["order_items"][0]["id"],
                product_name=object_value_dict["order_items"][0]["name"],
                product_price=object_value_dict["order_items"][0]["price"],
                active_from=datetime.strptime(object_value_dict["update_ts"], "%Y-%m-%d %H:%M:%S"),
                active_to=datetime(2099, 12, 31),
                restaurant_id=correct_restaurant_id,
            )
            

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                mapped_product = map_product(product)
                if mapped_product:
                    self.stg.insert_product(conn, mapped_product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

