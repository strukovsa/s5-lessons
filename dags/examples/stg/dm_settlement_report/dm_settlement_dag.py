import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.dm_users_dag.dm_users_loader import DmUsersLoader
from examples.stg.dm_restaurants_dag.dm_restaurants_loader import DmRestaurantsLoader
from examples.stg.dm_timestamps_dag.dm_timestamps_loader import DmTimestampsLoader
from examples.stg.dm_products_dag.dm_products_loader import DmProductsLoader
from examples.stg.dm_orders_dag.dm_orders_loader import DmOrdersLoader
from examples.stg.fct_product_sales_dag.fct_product_sales_loader import FctLoader
from examples.stg.dm_settlement_report.dm_settlement_loader import SetLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_dds_cdm_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        users_loader = DmUsersLoader(dwh_pg_connect, dwh_pg_connect, log)
        users_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="restaurants_load")
    def load_restaurants():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = DmRestaurantsLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()  # Вызываем функцию, которая перельет данные.

    @task(task_id="timestamps_load")
    def load_timestamps():
        # создаем экземпляр класса, в котором реализована логика.
        timestamps_loader = DmTimestampsLoader(dwh_pg_connect, dwh_pg_connect, log)
        timestamps_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    @task(task_id="products_load")
    def load_products():
        # создаем экземпляр класса, в котором реализована логика.
        products_loader = DmProductsLoader(dwh_pg_connect, dwh_pg_connect, log)
        products_loader.load_products()  # Вызываем функцию, которая перельет данные.

    @task(task_id="orders_load")
    def load_orders():
        # создаем экземпляр класса, в котором реализована логика.
        orders_loader = DmOrdersLoader(dwh_pg_connect, dwh_pg_connect, log)
        orders_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    @task(task_id="fct_load")
    def load_fct():
        # создаем экземпляр класса, в котором реализована логика.
        fct_loader = FctLoader(dwh_pg_connect, dwh_pg_connect, log)
        fct_loader.load_fct()  # Вызываем функцию, которая перельет данные.

    @task(task_id="set_load")
    def load_set():
        # создаем экземпляр класса, в котором реализована логика.
        set_loader = SetLoader(dwh_pg_connect, dwh_pg_connect, log)
        set_loader.load_set()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    users_dict = load_users()
    restaurants_dict = load_restaurants()
    timestamps_dict = load_timestamps()
    products_dict = load_products()
    orders_dict = load_orders()
    fct_dict = load_fct()
    set_dict = load_set()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    users_dict >> restaurants_dict >> timestamps_dict >> products_dict >> orders_dict >> fct_dict >> set_dict # type: ignore


stg_dds_cdm_dag = sprint5_example_dds_cdm_dag()

