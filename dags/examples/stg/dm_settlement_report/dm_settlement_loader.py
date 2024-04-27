from logging import Logger
from typing import List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import date


class SetObj(BaseModel):
    id: int
    restaurant_id: int
    restaurant_name: str
    settlement_date: date
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
       
                    from dds.fct_product_sales f
                    left join dds.dm_orders o on o.id = f.order_id
                    left join dds.dm_restaurants r on r.restaurant_id = o.restaurant_id
                    left join dds.dm_timsetamps t on t.timestamp_id = o.timestamp_id
                    WHERE o.restaurant_id > %(threshold)s AND o.order_status = 'CLOSED'
                    GROUP BY o.restaurant_id --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY o.restaurant_id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": rest_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SetDestRepository:

    def insert_set(self, conn: Connection, rest: SetObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s);
                """,
                {
                     "restaurant_id": rest.restaurant_id,
                     "restaurant_name": rest.restaurant_name,
                     "settlement_date": rest.settlement_date,
                     "orders_count": rest.orders_count,
                     "orders_total_sum": rest.orders_total_sum,
                     "orders_bonus_payment_sum": rest.orders_bonus_payment_sum,
                     "orders_bonus_granted_sum": rest.orders_bonus_granted_sum,
                     "order_processing_fee": rest.order_processing_fee,
                     "orders_restaurant_reward_sum": rest.restaurant_reward_sum

                    ,

                },
            )


class SetLoader:
    WF_KEY = "sales"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SetOriginRepository(pg_origin)
        self.stg = SetDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_set(self):
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
            load_queue = self.origin.list_set(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} set to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for rest in load_queue:
                self.stg.insert_set(conn, rest)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


