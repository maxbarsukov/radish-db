# Лабораторная работа 4

[![Elixir](https://github.com/maxbarsukov/radish-db/actions/workflows/elixir.yml/badge.svg?branch=master)](https://github.com/maxbarsukov/radish-db/actions/workflows/elixir.yml)
[![Markdown](https://github.com/maxbarsukov/radish-db/actions/workflows/markdown.yml/badge.svg?branch=master)](https://github.com/maxbarsukov/radish-db/actions/workflows/markdown.yml)
[![Coverage Status](https://coveralls.io/repos/github/maxbarsukov/radish-db/badge.svg?branch=master)](https://coveralls.io/github/maxbarsukov/radish-db?branch=master)

<img alt="Non Non Biyori GIF" src="./docs/img/non-non-biyori.gif" height="320">

> [!TIP]
> Yet Another Key-Value Database...

---

* Студент: `Барсуков Максим Андреевич`
* Группа: `P3315`
* ИСУ: `367081`
* Функциональный язык: `Elixir`

---

## Описание работы

**Цель**: получить навыки работы со специфичными для выбранной технологии/языка программирования приёмами.

### Требования

* программа должна быть реализована в функциональном стиле;
* требуется использовать идиоматичный для технологии стиль программирования;
* задание и коллектив должны быть согласованы;
* допустима совместная работа над одним заданием.

### Содержание отчёта

* титульный лист;
* требования к разработанному ПО, включая описание алгоритма;
* реализация с минимальными комментариями;
* ввод/вывод программы;
* выводы (отзыв об использованных приёмах программирования).

---

## Выполнение

_см. [README проекта](https://github.com/maxbarsukov/radish-db/blob/master/README.md)_

### Как запустить?

**1.** Склонировать репозиторий:

```bash
git clone git@github.com:maxbarsukov/radish-db.git
```

**2.** Установить зависимости:

```bash
mix deps.get
mix deps.compile
```

**3.** Линтинг:

```bash
mix check
```

**4.** Тестирование:

```bash
mix test --trace
```

**5.** Собрать `escript` — исполняемый файл, который может быть запущен на любой системе с установленным Erlang:

```bash
mix escript.build
```

**6.** Запустить приложение: TODO

### Форматирование, линтинг и тестирование

В рамках данной работы были применены инструменты:

* [ExUnit](https://hexdocs.pm/ex_unit/ExUnit.html) - для модульного тестирования;
* [Quixir](https://github.com/pragdave/quixir) - для тестирования свойств (property-based).
* [Credo](https://github.com/rrrene/credo) - инструмент статического анализа кода для языка Elixir;
* [Dialyxir](https://github.com/jeremyjh/dialyxir) - Dialy~~zer~~xir is a **DI**screpancy **A**na**LY**zer for ~~**ER**lang~~ Eli**XIR** programs.

---

## Выводы

TODO

---

## Полезные ссылки

| Ссылка | Описание |
| --- | --- |
| [elixirschool.com/otp_distribution](https://elixirschool.com/en/lessons/advanced/otp_distribution) | Распределение ОТП |
| [raft.github.io](https://raft.github.io/) | Raft Consensus Algorithm |
| [raft.github.io/raft.pdf](https://raft.github.io/raft.pdf) | In Search of an Understandable Consensus Algorithm (Extended Version) |
| [habr.com/ru/articles/469999/](https://habr.com/ru/companies/dododev/articles/469999/) | Как сервера договариваются друг с другом: алгоритм распределённого консенсуса Raft |
| [thesecretlivesofdata.com/raft/](https://thesecretlivesofdata.com/raft/) | Интерактивная демонстрация Raft |
| [en.wikipedia.org/wiki/Raft_(algorithm)](https://en.wikipedia.org/wiki/Raft_(algorithm)) | wiki: Raft|
| TODO | TODO |

## Лицензия <a name="license"></a>

Проект доступен с открытым исходным кодом на условиях [Лицензии MIT](https://opensource.org/license/mit/). \
_Авторские права 2024 Max Barsukov_

**Поставьте звезду :star:, если вы нашли этот проект полезным.**
