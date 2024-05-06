import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.api_restaurants.pg_saver import PgSaver
from examples.stg.api_restaurants.api_restaurants_loader import RestaurantsLoader
from examples.stg.api_restaurants.api_restaurants_reader import RestaurantsReader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_stg_api_restaurants():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    api_key = Variable.get("API_KEY")
    nickname = Variable.get("NICKNAME")
    cohort_number = Variable.get("COHORT_NUMBER")

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = RestaurantsReader(api_key, nickname, cohort_number)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantsLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    restaurants_loader = load_restaurants()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    restaurants_loader  # type: ignore


restaurants_stg_dag = sprint5_example_stg_api_restaurants()  # noqa
