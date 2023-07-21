# target-mysql

`target-mysql` is a Singer target for Oracle, Build with the [Meltano Target SDK](https://sdk.meltano.com).



English | [한국어](./docs/README_ko.md)


## Installation

Use PIP for installation:

```bash
pip install thk-target-mysql
```

Or use GitHub Repo:

```bash
pipx install git+https://github.com/thkwag/target-mysql.git@main
```

## Configuration

The available configuration options for `target-mysql` are:

| Configuration Options   | Description                                | Default            |
|-------------------------|--------------------------------------------|--------------------|
| host                    | MySQL server's hostname or IP address      |                    |
| port                    | Port where MySQL server is running         |                    |
| user                    | MySQL username                             |                    |
| password                | MySQL user's password                      |                    |
| database                | MySQL database's name                      |                    |
| table_name_pattern      | MySQL table name pattern                   | "${TABLE_NAME}"    |
| lower_case_table_names  | Use lowercase for table names or not       | true               |
| allow_column_alter      | Allow column alterations or not            | false              |
| replace_null            | Replace null values with others or not     | false              |

Configurations can be stored in a JSON configuration file and specified using the `--config` flag with `target-mysql`.

### The `replace_null` Option (Experimental)

By enabling the `replace_null` option, null values are replaced with 'empty' equivalents based on their data type. Use with caution as it may alter data semantics.

When `replace_null` is `true`, null values are replaced as follows:

| JSON Schema Data Type | Null Value Replacement |
|-----------------------|------------------------|
| string                | Empty string(`""`)     |
| number                | `0`                    |
| object                | Empty object(`{}`)     |
| array                 | Empty array(`[]`)      |
| boolean               | `false`                |
| null                  | null                   |


## Usage

```bash
cat <input_stream> | target-mysql --config <config.json>
```

- `<input_stream>`: Input data stream
- `<config.json>`: JSON configuration file

`target-mysql` reads data from a Singer Tap and writes it to a MySQL database. Run Singer Tap to generate data before launching `target-mysql`.

Here's an example of using Singer Tap with `target-mysql`:

```bash
tap-exchangeratesapi | target-mysql --config config.json
```

In this case, `tap-exchangeratesapi` is a Singer Tap that generates exchange rate data. The data is passed to `target-mysql` through a pipe(`|`), and `target-mysql` writes it to a MySQL database. `config.json` contains `target-mysql` settings.

## Developer Resources

### Initializing the Development Environment

```bash
pipx install poetry
poetry install
```

### Creating and Running Tests

Create tests in the `target_mysql/tests` subfolder and run:

```bash
poetry run pytest
```

Use `poetry run` to test `target-mysql` CLI interface:

```bash
poetry run target-mysql --help
```

### Testing with [Meltano](https://meltano.com/)

_**Note:** This target functions within a Singer environment and does not require Meltano._

Firstly, install Meltano and necessary plugins:

```bash
# Install Meltano
pipx install meltano

# Initialize Meltano in this directory
cd target-mysql
meltano install
```

Then, test and orchestrate with Meltano:

```bash
# Call tests:
meltano invoke target-mysql --version

# Or execute pipeline with Carbon Intensity sample tap:
meltano run tap-carbon-intensity target-mysql
```

### SDK Development Guide

For in-depth instructions on crafting Singer Taps and Targets using Meltano Singer SDK, see the [Development Guide](https://sdk.meltano.com/en/latest/dev_guide.html).

## Reference Links

- [Meltano Target SDK Documentation](https://sdk.meltano.com)
- [Singer Specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
- [Meltano](https://meltano.com/)
- [Singer.io](https://www.singer.io/)