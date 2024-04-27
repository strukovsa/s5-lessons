from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import datetime


class SetObj(BaseModel):
    id: int
    restaurant_id: int
    restaurant_name: str
    settlement_date: datetime
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float


class SetOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_set(self, rest_threshold: int, limit: int) -> List[SetObj]:
        with self._db.client().cursor(row_factory=class_row(SetObj)) as cur:
            cur.execute(
                """
                    select o.restaurant_id,
                        r.restaurant_name,
                        t.date as settlement_date,
                        count(f.order_id)) as orders_count,
                        SUM(f.total_sum) as orders_total_sum,
                        SUM(f.bonus_payment) as orders_bonus_payment_sum,
                        SUM(f.bonus_grant) as orders_bonus_granted_sum,
                        f.orders_total_sum * 0.25 as order_processing_fee,
                        (f.orders_total_sum * 0.75 - f.orders_bonus_payment_sum) as restaurant_reward_sum
       
                    from dds.fct_product_sales
                    left join dds.dm_restaurants r on r.restaurant_id = f.restaurant_id
                    left join dds.dm_orders o on o.order_id = f.order_id
                    left join dds.dm_timsetamps t on t.timestamp_id = o.timestamp_id
                        WHERE o.restaurant_id > %(threshold)s AND
                        o.order_status = 'CLOSED'
                    GROUP BY o.restaurant_id) --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY o.restaurant_id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": rest_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class FctDestRepository:

    def insert_fct(self, conn: Connection, rest: FctObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, current_timestamp, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s);
                """,
                {
                     "product_id": rest.product_id,
                     "order_id": rest.order_id,
                     "count": rest.count,
                     "price": rest.price,
                     "total_sum": rest.total_sum,
                     "bonus_payment": rest.bonus_payment,
                     "bonus_grant": rest.bonus_grant
                },
            )


class FctLoader:
    WF_KEY = "sales"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

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

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_fct(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} fct to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for rest in load_queue:
                self.stg.insert_fct(conn, rest)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


