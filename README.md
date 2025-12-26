# Taxi Trips Data Pipeline

[![license](https://img.shields.io/github/license/Scorpi-ON/Taxi-Trips-Data-Pipeline)](https://opensource.org/licenses/MIT)
[![Python versions](https://img.shields.io/badge/python-3.14-blue)](https://python.org/)
[![release](https://img.shields.io/github/v/release/Scorpi-ON/Taxi-Trips-Data-Pipeline?include_prereleases)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/releases)
[![downloads](https://img.shields.io/github/downloads/Scorpi-ON/Taxi-Trips-Data-Pipeline/total)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/releases)
[![code size](https://img.shields.io/github/languages/code-size/Scorpi-ON/Taxi-Trips-Data-Pipeline.svg)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline)

[![Ruff and Pyright checks](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/actions/workflows/linters.yaml/badge.svg)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/actions/workflows/linters.yaml)
[![CodeQL (Python, GH Actions)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/actions/workflows/codeql.yaml/badge.svg)](https://github.com/Scorpi-ON/Taxi-Trips-Data-Pipeline/actions/workflows/codeql.yaml)

[Тестовое задание](./TASK.docx) для трудоустройства в ООО «РХИТ»

<details>
  <summary><h2>Скриншоты</h2></summary>
</details>

## Стек

- **Python** — язык программирования
- **uv** — пакетный менеджер
- **Ruff** — инструмент для форматирования и анализа кода
- **Pyright** — статический типизатор Python
- **pre-commit** — фреймворк для настройки хуков Git

## Установка и запуск

0. Клонируйте репозиторий и перейдите в его папку.
1. Установите пакетный менеджер uv одним из способов. Например, для Windows:

```shell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

2. Установите зависимости:

```shell
uv sync --frozen --no-dev
```

3. Теперь запускать проект можно командой:

```shell
uv run -m src
```

## Модификация

Чтобы модифицировать проект, необходимо установить все зависимости, включая необходимые только для разработки:

```shell
uv sync
pre-commit install  # выполнять после активации venv 
```

Чтобы обновить датасет, нужно выполнить команду:
```shell
uv run -m src.data.loader
```

Запустить форматирование кода, его линтинг и статический анализ типов можно следующими командами соответственно:

```shell
ruff format
ruff check --fix
pyright
```

Эти операции выполняются автоматически при коммитах.
