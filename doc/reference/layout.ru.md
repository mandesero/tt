# Структура файловой системы {#filesystem-layout}

## Корень области установки {#the-root-of-a-scope}

У каждой [области установки](../concepts/projects-and-scopes.md#scopes) корень
устроен одинаково: `.tarantool/` проекта, корень области установки пользователя
или корень области установки системы.

```text
<root>/
├── bin/                                  # активные бинарные файлы (ссылки)
│   ├── tarantool
│   ├── tt
│   └── tcm
├── include/tarantool                     # заголовочные файлы активного tarantool (ссылка)
├── versions/
│   ├── tarantool/tarantool_3.8.1/        # tarantool, include/tarantool/
│   ├── tarantool-ee/tarantool-ee_3.8.1/
│   ├── tt/tt_3.0.0/
│   ├── tcm/tcm_1.3.1/
│   ├── tarantool-sdk/                    # какие компоненты SDK используются вместе
│   └── kv-api/kv-api_1.1.0/              # установленное приложение (user, system)
├── registry/                             # rock-сервер для tt wasm/emmylua prepare --from (project)
├── apps/
│   └── kv-api -> ../versions/kv-api/kv-api_1.1.0
├── etc/
│   └── kv-api/                           # пара хоста (user, system)
│       ├── config.yaml
│       ├── instances.yaml
│       └── systemd.yaml                  # экземпляры, которыми управляют юниты systemd
└── var/
    ├── lib/<app>/<instance>/             # данные; рабочий каталог внешнего процесса
    ├── log/<app>/<instance>/tt.log       # вывод экземпляра и сообщения его watchdog
    └── run/<app>/<instance>/             # tt.pid, tarantool.pid, tarantool.control,
                                          # tarantool.sock, process.pid
```

| Файл | |
| --- | --- |
| `run/.../tt.pid` | PID процесса-наблюдателя экземпляра |
| `run/.../tarantool.pid` | pid-файл, который записывает Tarantool |
| `run/.../tarantool.control` | сокет консоли, к которому подключается `tt connect <app>:<instance>` |
| `run/.../tarantool.sock` | бинарный порт, если конфигурация кластера или скрипт не слушают другой адрес |
| `run/.../process.pid` | PID внешнего процесса |
| `log/.../tt.log` | всё, что выводит экземпляр, и сообщения его процесса-наблюдателя |

Команда `tt wasm prepare --from` создаёт rock-сервер для локальной сборки
нативного модуля в `.tarantool/registry/` проекта.

## Проект {#a-project}

```text
myapp/
├── app.manifest.toml          # манифест пакета
├── app.manifest.lock          # зафиксированные версии зависимостей; сохраняем в git
├── VERSION                    # опционально: версия пакета
├── .templates/
│   └── default/               # шаблон экземпляров: пара проекта
│       ├── config.yaml
│       └── instances.yaml
├── .rocks/                    # зависимости и собранные компоненты (генерируется)
│   ├── share/tarantool/<name>/
│   └── bin/                   # программы, собранные компонентами
├── _build/pack/               # архивы, которые создаёт tt package pack (генерируется)
├── _build/rock/<product>/     # разложенные компоненты, устанавливаемые в .rocks/ (генерируется)
├── _build/rock/<product>.json # какой компонент положил туда каждый файл (генерируется)
├── _build/results.json        # что собрал каждый компонент: модули, файлы данных, программы (генерируется)
├── _build/cache/              # хеши и артефакты предыдущих инкрементальных сборок (генерируется)
├── _build/component/<component>/ # TT_BUILD_DIR каждого компонента: дерево сборки cmake, объектные файлы и кеш c и lua-c (генерируется)
├── _build/cargo/              # каталог target cargo-компонентов (генерируется)
├── _build/tt.lock             # блокировка проекта, которую сохраняет tt package clean (генерируется)
├── .env/                      # activate.sh, deactivate.sh (генерируется)
└── .tarantool/                # корень project scope (генерируется)
```

Сгенерированные каталоги не нужно добавлять в git:

```text
.rocks/
_build/
.env/
.tarantool/
```

## Компонент WebAssembly {#a-webassembly-component}

Команда `tt wasm new router --lang python` добавляет компонент в
`app.manifest.toml`. Пути от корня проекта:

```text
wasm.lock.json                    # профили компонентов, зафиксированные SDK и образы
wasm.local.json                   # настройки запуска на текущей машине (генерируется)
components/router/
├── .gitignore
├── main.py                       # исходный код Python
├── requirements.lock             # зафиксированные зависимости Python
├── wit/app.wit                   # WIT world
├── .tt-wasm/
│   ├── cache/                    # SDK и зависимости от tt wasm fetch
│   └── bindings/                 # привязки от tt wasm bind
└── dist/router.wasm              # собранный компонент
```

Добавьте в Git исходники, манифест, локфайлы и `.gitignore`. Исключите
локальные настройки запуска, кеш, привязки и результат сборки.

## Архив {#an-archive}

`tt package pack` собирает архив `<name>-<version>-<os>-<arch>.tt` в формате
tar.zst:

```text
app.manifest.toml
app.manifest.lock
VERSION
.templates/...
.rocks/...
_runtime/tarantool/bin/tarantool
_runtime/tt/bin/tt
```

В подписанном архиве также находятся файлы `hashes.json`, `env_hashes.json` и
соответствующие файлы `.sig`; см.
[Sign packages](../how-to/sign-packages.md).

## Пользовательский кэш {#the-user-cache}

`tt wasm` и `tt emmylua` хранят скачанное в подкаталоге `tt/`
пользовательского кэша: `$XDG_CACHE_HOME`, по умолчанию `~/.cache`, на
macOS - `~/Library/Caches`. Вне проектов `tt emmylua check`, `doc`, `fmt` и
`ls` запускают программы rock-пакета `emmylua` оттуда и пишут туда свою
конфигурацию, а не в текущий каталог (см.
[Check and format Lua code with EmmyLua](../how-to/emmylua.md#outside-a-project)):

```text
<user cache>/tt/emmylua/
├── <sha256>                          # файл релиза, скачанный tt emmylua rock
├── programs/<sha256>/                # bin/ рока с этим sha256
│   ├── emmylua_check
│   ├── emmylua_doc_cli
│   ├── emmylua_ls
│   └── luafmt
├── resolved/<key>.json               # рок, выбранный для сервера, версии и платформы
└── config/<key>/emmyrc.json          # конфигурация каталога, по его пути
```

Первый запуск скачивает рок и сообщает об этом; следующие находят его через
`resolved/` и к серверу не обращаются. Rock-сервер, который является
каталогом, опрашивается каждый раз. Чтобы скачать роки заново, удалите
этот каталог.
