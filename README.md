# Taxi Trips Data Pipeline

[![license](https://img.shields.io/github/license/Scorpi-ON/Taxi-Trips-Data-Pipeline)](https://opensource.org/licenses/MIT)
[![Python versions](https://img.shields.io/badge/python-3.12-blue)](https://python.org/)
[![release](https://img.shields.io/github/v/release/Scorpi-ON/Taxi-Trips-Data-Pipeline?include_prereleases)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/releases)
[![downloads](https://img.shields.io/github/downloads/Scorpi-ON/Taxi-Trips-Data-Pipeline/total)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/releases)
[![code size](https://img.shields.io/github/languages/code-size/Scorpi-ON/Taxi-Trips-Data-Pipeline.svg)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline)

[![Ruff and Pyright checks](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/actions/workflows/linters.yaml/badge.svg)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/actions/workflows/linters.yaml)
[![CodeQL (Python, GH Actions)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/actions/workflows/codeql.yaml/badge.svg)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/actions/workflows/codeql.yaml)

End-to-end конвейер обработки данных из датасета ["NYC Taxi & Limousine Commission"](https://www.kaggle.com/datasets/raminhuseyn/new-york-city-taxi-and-limousine-project/data) на базе Airflow + dbt + Superset. [Тестовое задание](./TASK.docx) для трудоустройства в ООО «РХИТ». 

## Как это работает

Каждый раз при запуске проекта выполняются все этапы конвейера. После инициализации необходимых сервисов Airflow запускает требуемый в задаче [DAG](./dags/nyc_taxi_pipeline.py), который вначале осуществляет загрузку сырых данных в PostgreSQL из [CSV-датасета](./data/nyc_taxi_raw.csv) (максимум 50000 строк). Они загружаются как есть: с сохранением заданных в датасете имён столбцов и в текстовом виде. Перед каждой загрузкой БД `raw.nyc_taxi_trips` очищается для обеспечения идемпотентности. В конце выводится количество строк в файле и количество загруженных строк.

<img width="70%" src="https://github.com/user-attachments/assets/2c6c70b8-2874-4b0d-80be-87eb7542a39e" />

Далее производится трансформация и тестирование данных с помощью dbt, установленного в контейнер Airflow как зависимость. Запускается слой staging, где из сырых данных отбираются нужные поля, приводятся к правильным типам и валидируются согласно ТЗ: отбрасываются записи с неуказанной датой поездки (trip_date), вместо отрицательных / нулевых значений прибыли (fare amount и total_amount) указывается null. Результат выполнения этого слоя сохраняется как представление `stg.stg_nyc_taxi_trips`. 

На слое marts формируется таблица `marts.mart_taxi_daily`, используемая для аналитики и содержащая агрегированные по датам поездок данные. 

После обоих слоёв выполняются минимальные тесты на корректность: проверка уникальности идентификатора поездки (trip_id) и проверка на null полей суммы выручки (revenue_sum) и даты поездки (trip_date).

Параллельно с выполнением DAG производится импорт заранее подготовленного [дашборда](./superset/assets/nyc_taxi_trips.zip) в Superset, уже подключённого к базе данных PostgreSQL. Подключение осуществляется по стандартным данным PostgreSQL, указанным в [.env.example](./.env.example). При экспорте они остаются захардкоженными, поэтому если вы их измените, дашборд не сможет подключиться корректно. Исключение составляет пароль, который подхватывается при каждом новом запуске динамически. После выполнения DAG на нём можно будет посмотреть отображены графики выручки и числа поездок по дням за октябрь — декабрь 2017.

<img width="70%" src="https://github.com/user-attachments/assets/32a845bc-7379-484b-bc40-7be294e97506" />

## Стек

- **Apache Airflow** — фреймворк для оркестрации процессов
- **PostgreSQL** — хранилище данных
-	**dbt** — фреймворк для трансформации и контроля качества данных
-	**Apache Superset** — инструмент визуализации
- **Python** — язык программирования
- **uv** — пакетный менеджер
- **Ruff** — инструмент для форматирования и анализа кода
- **Pyright** — статический типизатор
- **pre-commit** — фреймворк для настройки хуков Git

## Установка и запуск

0. Клонируйте репозиторий и перейдите в его папку.
1. Создайте файл `.env` на основе [.env.example](.env.example) и при необходимости измените указанные там параметры.
2. Установите Docker.
3. Теперь запускать проект можно командой:

```shell
docker compose up -d --build
```

Теперь Airflow будет доступен по адресу http://localhost:8080/, а Superset — http://localhost:8088. К БД PostgreSQL можно подключиться на localhost:5432. Логины и пароли для входа заданы в [.env](./.env).

## Модификация

Чтобы модифицировать проект, установите пакетный менеджер uv одним из способов. Например, для Windows:

```shell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Далее необходимо установить зависимости:

```shell
uv sync
pre-commit install  # эту и последующие команды выполнять после активации venv 
```

Чтобы обновить датасет, нужно выполнить команду:
```shell
uv run -m data.loader
```

Для запуска трансформаций и тестов dbt, запустите PostgreSQL, раскомментируйте переменную `POSTGRES_HOST` в [.env](./.env) и выполните команды:
```shell
cd dbt
dbt run --profiles-dir . --select stg_nyc_taxi_trips
dbt test --profiles-dir . --select stg_nyc_taxi_trips
dbt run --profiles-dir . --select marts_nyc_taxi_trips
dbt test --profiles-dir . --select marts_nyc_taxi_trips                  
```

Запустить форматирование кода, его линтинг и статический анализ типов (эти операции выполняются автоматически при коммитах) можно следующими командами соответственно:

```shell
ruff format
ruff check --fix
pyright
```
