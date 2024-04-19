import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.dm_users_dag.dm_users_loader import DmUsersLoader
from examples.stg.dm_restaurants_dag.dm_restaurants_loader import DmRestaurantsLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_dds_restaurants_dag():
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

    # Инициализируем объявленные таски.
    users_dict = load_users()
    restaurants_dict = load_restaurants()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    users_dict >> restaurants_dict  # type: ignore


stg_dds_restaurants_dag = sprint5_example_dds_restaurants_dag()

