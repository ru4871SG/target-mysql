"""Oracle target class."""

from __future__ import annotations

from singer_sdk.target_base import SQLTarget
from singer_sdk import typing as th

from target_oracle.sinks import (
    OracleSink,
)


class TargetOracle(SQLTarget):
    """Sample target for Oracle."""

    name = "target-oracle"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="SQLAlchemy connection string",
        ),
    ).to_dict()

    default_sink_class = OracleSink


if __name__ == "__main__":
    TargetOracle.cli()
