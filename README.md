# target-mysql

`target-mysql` is a Singer target for Oracle, Build with the [Meltano Target SDK](https://sdk.meltano.com).


<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install target-oracle
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/target-oracle.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the target.

This section can be created by copy-pasting the CLI output from:

```
target-oracle --about --format=markdown
```
-->

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