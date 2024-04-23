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

class FctObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime
   
class FctDdsObj(BaseModel):
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float

class FctOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_fct(self, order_threshold: int, limit: int) -> List[FctObj]:
        with self._db.client().cursor(row_factory=class_row(FctObj)) as cur:
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
    
    def get_product_id(self, product_id: str) -> Optional[int]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_products
                    WHERE product_id = %(product_id)s
                    AND active_to = '2099-12-31 00:00:00.000';
                """,
                {"product_id": product_id},
            )
            result = cur.fetchone()
            if result:
                return result[0]
        return None
    
    def get_order_id(self, order_id: str) -> Optional[int]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT id
                    FROM dds.dm_orders
                    WHERE order_key = %(order_id)s;
                """,
                {"order_id": order_id},
            )
            result = cur.fetchone()
            if result:
                return result[0]
        return None
    
    def get_bonus_payment(self, order_id: str) -> Optional[int]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT json_path_query_array(event_value::json, '$.product_payments[*].bonus_payment') AS bonus_payment
                    FROM stg.bonussystem_events
                    WHERE event_value::json->>'order_id' = %(order_id)s;

                """,
                {"order_id": order_id},
            )
            result = cur.fetchone()
            if result:
                return result[0]
        return None
    
    def get_bonus_grant(self, order_id: str) -> Optional[int]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                    SELECT json_path_query_array(event_value::json, '$.product_payments[*].bonus_grant') AS bonus_grant
                    FROM stg.bonussystem_events
                    WHERE event_value::json->>'order_id' = %(order_id)s;

                """,
                {"order_id": order_id},
            )
            result = cur.fetchone()
            if result:
                return result[0]
        return None

class FctDestRepository:

    def insert_fct(self, conn: Connection, fcts: List[FctDdsObj]) -> None:
        with conn.cursor() as cur:
            for fct in fcts:
                cur.execute("""
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s);
                    """,
                    {
                     "product_id": fct.product_id,
                     "order_id": fct.order_id,
                     "count": fct.count,
                     "price": fct.price, 
                     "total_sum": fct.total_sum,
                     "bonus_payment": fct.bonus_payment,
                     "bonus_grant": fct.bonus_grant,

                    },
                    )


class FctLoader:
    WF_KEY = "example_fct_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 3000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = FctOriginRepository(pg_origin)
        self.stg = FctDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_fct(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Map FctObj to FctDdsObj
            def map_fct(fct: FctObj) -> List[FctDdsObj]:
                object_value_dict = json.loads(fct.object_value)
                product_id = [item["id"] for item in object_value_dict["order_items"]]
                correct_product_id = [self.origin.get_product_id(product_id) for product_id in product_id]
                if correct_product_id is None:
                    raise ValueError(f"Could not find a matching product_id for {product_id}.")
                order_id = fct.object_id
                correct_order_id = self.origin.get_order_id(order_id)
                old_order_id = fct.object_id
                if correct_order_id is None:
                    raise ValueError(f"Could not find a matching order_id for {order_id}.")

                mapped_fcts = []
                for product_id, count, price in zip(correct_product_id, count, price):
                    if product_id is not None:
                        mapped_fcts.append(
                        FctDdsObj(
                        order_id=correct_order_id,
                        product_id=correct_product_id,
                        count=[item["quantity"] for item in object_value_dict["order_items"]],
                        price=[item["price"] for item in object_value_dict["order_items"]],
                        total_sum=count * price,
                        bonus_payment=self.origin.get_bonus_payment(old_order_id),
                        bonus_grant=self.origin.get_bonus_grant(old_order_id),  
                        )
                        )
                return mapped_fcts
                
            

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_fct(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fcts to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for fct in load_queue:
                mapped_fct = map_fct(fct)
                if mapped_fct:
                    self.stg.insert_fct(conn, mapped_fct)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

