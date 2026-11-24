# Запуск компонентов WebAssembly (экспериментально) {#запуск-компонентов-webassembly-экспериментально}

[tarantool-wasm](https://github.com/tarantool/tarantool-wasm-rs) запускает
компоненты WebAssembly внутри Tarantool. Код на Lua обращается к ним через
`require('wasm')`. Стандартный модуль `wasm` предоставляет команды `tt wasm`
и бэкенд сборки `wasm`. В этом руководстве мы соберём функцию на Python и
вызовем её из Lua. Пример приложения целиком приведён в руководстве
[HTTP API в компоненте WebAssembly](../write-your-app/http-api-in-wasm.md).

Для запуска компонента нужен Tarantool 3.6 или новее на Linux x86-64 или
macOS arm64.

## Подготовка проекта {#prepare-the-project}

По умолчанию tt запускает инструменты сборки в Docker. Загрузите
`builder-images.json` из [релиза tt-wasm](https://github.com/mandesero/tt-wasm/releases/tag/v0.1.0).
В каталоге приложения с `app.manifest.toml` выберите образ Python по дайджесту:

```console
$ image=$(jq -er '.images[] | select(.target == "python") | .reference' builder-images.json)
$ tt wasm prepare --lang python --image "$image"
```

Чтобы собрать образ для разработки, из корня репозитория tt загрузите
[модуль tt-wasm](https://github.com/mandesero/tt-wasm) в версии из `go.mod`
и соберите образ Python с SDK из tt:

```console
$ go mod download github.com/mandesero/tt-wasm
$ wasm_dir=$(go list -m -f '{{.Dir}}' github.com/mandesero/tt-wasm)
$ docker build --build-context "tt-sdk=$PWD/sdk" --platform linux/amd64 \
    --target python -t tt-wasm-builder:dev "$wasm_dir"
```

Выберите этот образ из каталога приложения:

```console
$ tt wasm prepare --lang python --image tt-wasm-builder:dev
```

`prepare` объявляет зависимость от rock-модуля `wasm`, пересчитывает локфайл
пакета и проверяет выбранные инструменты. Индекс rock-сервера содержит сборки
нативного модуля для поддерживаемых платформ. У образа сборщика своя
платформа `linux/amd64`, независимая от платформы запуска приложения.

## Наборы инструментов {#the-toolchains}

Инструменты Python входят в Docker-образ. Чтобы собирать компоненты прямо
на хосте, установите следующие утилиты и добавьте их в `PATH`:

| Инструмент | Версия |
| --- | --- |
| `python3` с pip | 3.10 или новее |
| `componentize-py` | 0.25.1 |
| `wasm-tools` | 1.261.0 |
| `wkg` | 0.16.1 |

Затем выполните:

```console
$ tt wasm prepare --lang python --runner local
```

`prepare` проверяет локальные утилиты. Если инструмента не хватает или его
версия не подходит, установите нужную версию самостоятельно. После
подготовки команды одинаковы для обоих способов сборки.

## Добавление компонента {#add-a-component}

```console
$ tt wasm new calc --lang python
$ tt wasm prepare
$ tt wasm fetch calc
```

`new` создаёт компонент в `components/calc/` и объявляет его в манифесте.
Повторный `prepare` фиксирует в `wasm.lock.json` профиль инструментария,
версию модуля исполнения и контрольную сумму SDK. Для Docker-профиля он
также фиксирует образ сборщика. `fetch` загружает SDK и зависимости из
`wkg.lock` и `requirements.lock`, если эти файлы есть. Для зависимостей
Python укажите точные версии и хеши wheel-пакетов на чистом Python.

```toml
[components.calc]
path = "components/calc"

[components.calc.build]
backend = "wasm"
language = "python"
source = "main.py"
wit = "wit"
world = "calc"
profile = "python-ttwasm-0.1.0-r1"
output = "dist/calc.wasm"
```

Добавьте в Git `app.manifest.lock`, `wasm.lock.json`, исходники и локфайлы
зависимостей. Исключите `wasm.local.json` и генерируемые каталоги
`.tt-wasm/` и `dist/`. Локально собранный Docker-образ закрепляется по
идентификатору образа. Он должен быть доступен в Docker-контексте на каждой
машине сборки.

## Другие языки

Для других языков порядок тот же. При локальной сборке Docker-образа
в `--target` укажите стадию Dockerfile для выбранного тулчейна. В примере
собираем калькулятор на JavaScript.

```console
$ docker build --build-context "tt-sdk=$PWD/sdk" --platform linux/amd64 \
    --target js -t tt-wasm-builder:js "$wasm_dir"
$ tt wasm prepare --lang js --image tt-wasm-builder:js
$ tt wasm new calc --lang js
$ tt wasm prepare
$ tt wasm fetch calc
$ tt wasm bind calc
$ tt package build --locked
```

| `--lang` | Стадия Dockerfile | Локальные инструменты | Файлы зависимостей |
| --- | --- | --- | --- |
| `python` | `python` | Python 3.10+, pip, componentize-py 0.25.1 | `requirements.lock` |
| `js` | `js` | Node.js 24+, Jco 1.37.0, ComponentizeJS 0.23.0 | `package-lock.json` |
| `go` | `go` | Go 1.27.x, TinyGo 0.43.0, Binaryen 133, wit-bindgen-go 0.7.0 | `go.mod`, `go.sum` |
| `rust` | `rust` | Rust 1.96+, cargo-component 0.21.1, `wasm32-wasip1` | `Cargo.lock` |
| `c`, `cpp` | `c` | WASI SDK 34.0, wit-bindgen 0.62.0 | `wkg.lock` |
| `csharp` | `csharp` | .NET SDK 10.0.402, WASI SDK 29.0, wit-bindgen 0.58.0 | `packages.lock.json` |

Для всех локальных тулчейнов также нужны `wasm-tools` 1.261.0 и `wkg`
0.16.1. Для C# в local-режиме нужна Linux amd64. Укажите корень WASI SDK
в `WASI_SDK_PATH`. Внешние WIT-пакеты можно зафиксировать в `wkg.lock`.

## Изменение зависимостей {#change-dependencies}

Обновите манифест и локфайл менеджером пакетов в каталоге компонента.
Сохраните оба файла в системе контроля версий. Вернитесь в каталог
приложения и выполните `tt wasm fetch calc`, чтобы загрузить зависимости
для генерации привязок и сборки. Например, для JavaScript используйте
`npm install --package-lock-only lodash-es@4.17.23`, а для Rust —
`cargo add serde@1.0.228`. Менеджерам пакетов Go и Rust нужен SDK,
который публикует `tt wasm bind`. Если в старом Rust-компоненте в
`Cargo.toml` указан путь `.tt-wasm-sdk-rust`, замените его на
`.tt-wasm/bindings/tarantool-sdk` и выполните `tt wasm fetch calc` и
`tt wasm bind calc` перед обновлением локфайла Cargo.
Для C# после изменения ссылок на пакеты пересоздайте локфайл командой
`dotnet restore -p:RestoreLockedMode=false`.

В `requirements.lock` укажите точные версии и хеши всех Python-пакетов,
включая транзитивные зависимости. Используйте wheels на чистом Python,
совместимые с версией Python гостевой среды. Менеджер пакетов npm
устанавливает зафиксированные рабочие зависимости без запуска скриптов
жизненного цикла. Для C и C++ можно использовать заголовки из каталога
компонента или отдельно скомпилировать WASI-библиотеки и передать их
Clang в аргументах сборки.
Соберите библиотеки для WASI Preview 2 с помощью WASI SDK и поместите
заголовки и архивы в каталог компонента. В обеих средах относительные
пути компилятора и компоновщика отсчитываются от каталога компонента.

```toml
[components.calc.build.args]
build = ["-Ivendor/include", "-Lvendor/lib", "-lxxhash"]
```

В режиме Docker команда `fetch` запускает менеджеры пакетов в контейнере
и сохраняет кэш в проекте. Повторные сборки в новых контейнерах используют
этот кэш. При смене среды выполните `fetch` заново. Например, чтобы
переключить JS-компонент с Docker на локальную сборку:

```console
$ tt wasm prepare --lang js --runner local
$ tt wasm fetch calc
$ tt wasm bind calc
$ tt package build --locked
```

`tt wasm prepare --lang js --runner docker` возвращает ранее
зафиксированный образ. Параметр `--runner` у команд `fetch`, `bind` и
`build` выбирает среду для одного вызова. Для каждой среды используется
свой кэш зависимостей. Перед проверкой привязок сгенерируйте их заново
для выбранной среды.

## Написание компонента {#write-it}

В файле `components/calc/wit/app.wit` объявлен экспорт:

```wit
package example:calc@0.1.0;

world calc {
    include tarantool:tarantool/guest@0.2.0;
    export add: func(a: s32, b: s32) -> s32;
}
```

Его реализация находится в `components/calc/main.py`:

```python
import wit_world


class WitWorld(wit_world.WitWorld):
    def add(self, a: int, b: int) -> int:
        return a + b
```

`tt wasm bind calc` записывает привязки для редактора и проверки
в `components/calc/.tt-wasm/bindings/`. Во время сборки tt генерирует
необходимые привязки во временном каталоге. После изменения WIT вызовите
`tt wasm bind calc` и обновите реализацию. Если изменилась зависимость WIT
или её локфайл, сначала выполните `tt wasm fetch calc`.

`tt wasm clean calc` удаляет сгенерированные привязки и результаты сборки.
Затем можно заново создать привязки и собрать компонент с уже загруженными
зависимостями.

## Сборка и вызов из Lua {#build-it-and-call-it-from-lua}

```console
$ tt package build --locked
```

Команда `tt wasm build calc` собирает только этот компонент. При успешной
сборке бэкенд записывает `components/calc/dist/calc.wasm` и помещает его в
пространство имён пакета. Для пакета `myapp` это
`.rocks/share/tarantool/myapp/dist/calc.wasm`. При ошибке компиляции или
проверки предыдущий файл компонента сохраняется.

Из каталога приложения выполните:

```console
$ tt run -e "local wasm = require('wasm') \
    local calc = wasm.load('.rocks/share/tarantool/myapp/dist/calc.wasm') \
    print(wasm.call(calc, 'add', 20, 22)) os.exit()"
42
```

В путях поиска Lua-модулей Tarantool использует символы `;` и `?`.
Выберите путь к приложению без этих символов, чтобы `require('wasm')`
находил установленный модуль стандартным способом.

`wasm.bind(calc)` предоставляет функции Lua для экспортов компонента.
`wasm.drop(calc)` освобождает компонент, который больше не нужен.

Docker запускает `bind` и `build` без сети. Для автономной сборки или
упаковки сначала загрузите зависимости компонента командой `tt wasm fetch`
и сделайте локальное зеркало rock-модуля `wasm`. Версию возьмите из
`app.manifest.lock`, исходный rock-сервер — из зависимости `wasm` в
`app.manifest.toml`. Для стандартного rock-сервера и версии из примера:

```console
$ registry=https://raw.githubusercontent.com/tarantool-contrib/rocks-repo/index/
$ tt package download wasm@0.1.0-1 --registry "$registry" --dir ./vendor-rocks
$ tt wasm prepare --rock-server ./vendor-rocks
$ tt package pack --locked --registry ./vendor-rocks
```

`prepare --rock-server` меняет явный rock-сервер зависимости `wasm`
и сохраняет её ограничение версии. Если у приложения есть другие
rock-зависимости, загрузите их в зеркало. См.
[Зеркало для автономной сборки](../concepts/rock-servers.md#a-mirror-for-offline-builds).

## Проверка проекта {#check-the-project}

```console
$ tt wasm status
$ tt wasm doctor --component calc
$ tt wasm bind calc --check
```

`status` показывает объявленные компоненты, профили, зависимости и состояние
привязок. `doctor` дополнительно проверяет выбранные инструменты.
`bind --check` выявляет отсутствующие, изменённые или устаревшие привязки.

## Упаковка {#pack-it}

По умолчанию tt упаковывает компонент с зависимостями, в том числе с
нативным модулем для целевой платформы. В примере используется Linux x86-64.

```console
$ tt package pack --locked
$ tt package install _build/pack/myapp-1.0.0-linux-amd64.tt --scope user --yes
```

При упаковке с `--without-deps` установщик получает подходящий rock-модуль
с rock-сервера и сверяет его с локфайлом архива.

## Публикация rock {#share-the-rock}

`tt wasm rock` упаковывает известный релиз нативного модуля для каждой
платформы и добавляет файлы, необходимые для раздачи каталога как rock-сервера:

```console
$ tt wasm rock --version 0.1.0 --out dist/rocks
```

`tt wasm prepare --rock-server URL` позволяет объявить зависимость от
rock-модуля с другого сервера. Для локальной сборки модуля выполните:

```console
$ tt wasm prepare --from target/release/libtarantool_wasm_rs.so
```

Команда упаковывает локальную сборку для платформы хоста в
`.tarantool/registry`. Чтобы брать нативные модули и SDK с зеркала, см.
[формат списка релизов](../reference/environment.md#webassembly-components).

## Команды и параметры {#commands-and-options}

| Команда | Назначение |
| --- | --- |
| `tt wasm prepare` | Объявить зависимость от нативного модуля и подготовить профили сборки |
| `tt wasm new NAME --lang python` | Создать и объявить компонент Python |
| `tt wasm fetch [NAME]` | Загрузить зафиксированные SDK и зависимости |
| `tt wasm bind [NAME] [--check]` | Сгенерировать или проверить языковые привязки |
| `tt wasm build NAME` | Собрать один компонент |
| `tt wasm status` | Показать состояние проекта |
| `tt wasm doctor [--component NAME] [--json]` | Проверить проект и инструменты |
| `tt wasm clean [NAME]` | Удалить сгенерированные файлы |
| `tt wasm rock --out DIR` | Упаковать нативные модули для rock-сервера |

`new --dir` задаёт каталог компонента, `new --world` задаёт имя WIT world.
`new --template` принимает `call` или `http` и выводит соответствующий
пример на Lua. `prepare --update` обновляет зафиксированный профиль.
`tt wasm build NAME -- --help` выводит справку инструмента сборки.
