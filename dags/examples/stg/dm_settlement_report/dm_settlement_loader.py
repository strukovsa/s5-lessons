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

    def list_set(self, set_threshold: int, limit: int) -> List[SetObj]:
        with self._db.client().cursor(row_factory=class_row(SetObj)) as cur:
            cur.execute(
                """
                    select f.m_id as id,
                        o.restaurant_id,
                        r.restaurant_name,
                        t.date as settlement_date,
                        count(f.order_id) as orders_count,
                        SUM(f.sum) as orders_total_sum,
                        SUM(f.b_payment) as orders_bonus_payment_sum,
                        SUM(f.b_grant) as orders_bonus_granted_sum,
                        (SUM(f.sum) * 0.25) as order_processing_fee,
                        (SUM(f.sum) * 0.75 - SUM(f.b_payment)) as restaurant_reward_sum

                    from (SELECT MAX(id) as m_id,
                            order_id,
                            SUM(total_sum) sum,
                            MAX(bonus_payment) as b_payment,
                            MAX(bonus_grant) as b_grant
                            from dds.fct_product_sales
                            where order_id IN (SELECT id
                                                FROM dds.dm_orders
                                                WHERE order_status = 'CLOSED')
                            group by order_id) f
                    left join dds.dm_orders o on o.id = f.order_id
                    left join dds.dm_restaurants r on r.id = o.restaurant_id
                    left join dds.dm_timestamps t on t.id = o.timestamp_id
                    WHERE f.m_id > %(threshold)s AND o.order_status = 'CLOSED'
                    GROUP BY f.m_id, o.restaurant_id, r.restaurant_name, t.date
                    ORDER BY o.restaurant_id ASC
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": set_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SetDestRepository:

    def insert_set(self, conn: Connection, set: SetObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s)
                    ON CONFLICT (restaurant_id, settlement_date)
                    DO UPDATE SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                     "restaurant_id": set.restaurant_id,
                     "restaurant_name": set.restaurant_name,
                     "settlement_date": set.settlement_date,
                     "orders_count": set.orders_count,
                     "orders_total_sum": set.orders_total_sum,
                     "orders_bonus_payment_sum": set.orders_bonus_payment_sum,
                     "orders_bonus_granted_sum": set.orders_bonus_granted_sum,
                     "order_processing_fee": set.order_processing_fee,
                     "restaurant_reward_sum": set.restaurant_reward_sum

                    ,

                },
            )
            


class SetLoader:
    WF_KEY = "example_dds_cdm_dag"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 4000  # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

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
            for set in load_queue:
                self.stg.insert_set(conn, set)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


