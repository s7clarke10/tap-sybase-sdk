# tap-sybase

`tap-sybase` is a Singer tap for sybase.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

[Singer](https://www.singer.io/) tap that extracts data from a [sybase](https://infocenter.sybase.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md). This TAP was built against Sybase Version 16 also known as Adapter Server Enterprise or officially as SAP Adaptive Server Enterprise (ASE) 16.0. Compatibility with older versions of Sybase is unknown, I would recommend testing the TAP for compatibility.

## Installation

Install from PyPi:

```bash
pipx install tap-sybase
```

Install from GitHub:

```bash
pipx install git+https://github.com/s7clarke10/tap-sybase-sdk.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-sybase --about --format=markdown
```
-->

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| user                | True     | None    | The user name used to connect to the Sybase database |
| password            | True     | None    | The password for the connecting user to the Sybase database |
| host                | True     | None    | The host name or IP Address running the Sybase Database |
| port                | False    |    2638 | The port that the database is running on, defaults to port 2638 |
| filter_dbs          | False    | None    | To filter the discovery to a particular schema within a database. This is useful if you have a large number of schemas and wish to speed up the discovery. |
| use_date_datatype   | False    | False   | To emit a date as a date without a time component or time without an UTC offset. This is helpful to avoid time conversions or to just work with a date datetype in the target database. If this boolean config item is not set, the default behaviour is `false` i.e. emit date datatypes as a datetime. It is recommended to set this on if you have time datetypes and are having issues uploading into into a target database. |
| tds_version         | False    | None    | Set the version of TDS to use when communicating with Sybase Server (the default is None). This is used by pymssql with connecting and fetching data from Sybase databases. See the pymssql documentation and FreeTDS documentation for more  details. |
| characterset        | False    | utf8    | The characterset for the database / source system. The default is `utf8`, however older databases might use a charactersets like cp1252 for the encoding. If you have errors with a UnicodeDecodeError: 'utf-8' codec can't decode byte .... then a solution is examine the characterset of the source database / system and make an appropriate substitution for utf8 like cp1252. |
| cursor_array_size   | False    |       1 | To make use of fetchmany(x) instead of fetchone(), use cursor_array_size with an integer value indicating the number of rows to pull. This can help in some architectures by pulling more rows into memory. The default if omitted is 1, the tap will still use fetchmany, but with an argument of 1, under the assumption that  like cp1252. |
| use_singer_decimal  | False    | False   | o emit all numeric values as strings and treat floats as string data types for the target, set use_singer_decimal to true. The resulting SCHEMA message will contain an attribute in additionalProperties containing the scale and precision of the discovered property |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-sybase --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-sybase` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-sybase --version
tap-sybase --help
tap-sybase --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_sybase/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-sybase` CLI interface directly using `poetry run`:

```bash
poetry run tap-sybase --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-sybase
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-sybase --version
# OR run a test `elt` pipeline:
meltano elt tap-sybase target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
