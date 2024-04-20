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

class DmOrdersObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime
   
class DmOrdersDdsObj(BaseModel):
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int

class DmOrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int, limit: int) -> List[DmOrdersObj]:
        with self._db.client().cursor(row_factory=class_row(DmOrdersObj)) as cur:
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
                    "threshold": order_threshold,
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
    
    def get_timestamp_id(self, timestamp_id: str) -> Optional[int]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_timestamps
                    WHERE ts = %(timestamp_id)s;
                """,
                {"timestamp_id": timestamp_id},
            )
            result = cur.fetchone()
            if result:
                return result[0]
        return None
    
    def get_user_id(self, user_id: str) -> Optional[int]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_users
                    WHERE user_id = %(user_id)s;
                """,
                {"user_id": user_id},
            )
            result = cur.fetchone()
            if result:
                return result[0]
        return None


class DmOrdersDestRepository:

    def insert_orders(self, conn: Connection, order: DmOrdersDdsObj) -> None:
        with conn.cursor() as cur:

            cur.execute("""
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id)
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s);
                """,
                {
                     "order_key": order.order_key,
                     "order_status": order.order_status,
                     "restaurant_id": order.restaurant_id,
                     "timestamp_id": order.timestamp_id, 
                     "user_id": order.user_id
                },
            )


class DmOrdersLoader:
    WF_KEY = "example_orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 3000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DmOrdersOriginRepository(pg_origin)
        self.stg = DmOrdersDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Map DmOrdersObj to DmOrdersDdsObj
            def map_order(order: DmOrdersObj) -> DmOrdersDdsObj:
                object_value_dict = json.loads(order.object_value)
                restaurant_id = object_value_dict["restaurant"]["id"]
                correct_restaurant_id = self.origin.get_restaurant_id(restaurant_id)
                if correct_restaurant_id is None:
                    raise ValueError(f"Could not find a matching restaurant_id for {restaurant_id}.")
                date_str = object_value_dict["date"]
                timestamp_id = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                correct_timestamp_id = self.origin.get_timestamp_id(timestamp_id)
                if correct_timestamp_id is None:
                    raise ValueError(f"Could not find a matching timestamp_id for {restaurant_id}.")
                user_id = object_value_dict["user"]["id"]
                correct_user_id = self.origin.get_user_id(user_id)
                if correct_user_id is None:
                    raise ValueError(f"Could not find a matching user_id for {user_id}.")

                return DmOrdersDdsObj(
                order_key=order.object_id,
                order_status=object_value_dict["final_status"],
                restaurant_id=correct_restaurant_id,
                timestamp_id=correct_timestamp_id,
                user_id=correct_user_id
            )
            

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                mapped_order = map_order(order)
                if mapped_order:
                    self.stg.insert_orders(conn, mapped_order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

