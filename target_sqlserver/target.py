"""SqlServer target class."""

from __future__ import annotations

import typing as t

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_sqlserver.sinks import SqlServerSink

if t.TYPE_CHECKING:
    from pathlib import PurePath


class SqlServerTarget(SQLTarget):
    """Target for SqlServer."""

    package_name = "lumenn-target-sqlserver"

    def __init__(
        self,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Initialize the target.

        Args:
            config: Target configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        self.max_parallelism = 1
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )
        # There's a few ways to do this in JSON Schema but it is schema draft dependent.
        # https://stackoverflow.com/questions/38717933/jsonschema-attribute-conditionally-required # noqa: E501
        assert (self.config.get("sqlalchemy_url") is not None) or (
            self.config.get("host") is not None
            and self.config.get("port") is not None
            and self.config.get("user") is not None
            and self.config.get("password") is not None
            and self.config.get("dialect+driver") is not None
        ), (
            "Need either the sqlalchemy_url to be set or host, port, user,"
            + "password, dialect+driver to be set"
        )

        assert self.config.get("add_record_metadata") or not self.config.get(
            "activate_version"
        ), (
            "Activate version messages can't be processed unless add_record_metadata "
            "is set to true. To ignore Activate version messages instead, Set the "
            "`activate_version` configuration to False."
        )

    name = "target-sqlserver"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description=(
                "Hostname for sqlserver instance. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=1433,
            description=(
                "The port on which sqlserver is awaiting connection. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "user",
            th.StringType,
            description=(
                "User name used to authenticate. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "password",
            th.StringType,
            description=(
                "Password used to authenticate. "
                "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "database",
            th.StringType,
            description=(
                "Database name. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            description=(
                "SQLAlchemy connection string. "
                + "This will override using host, user, password, port, "
                + "dialect, and all ssl settings. Note that you must escape password "
                + "special characters properly. See "
                + "https://docs.sqlalchemy.org/en/20/core/engines.html#escaping-special-characters-such-as-signs-in-passwords"
            ),
        ),
        th.Property(
            "dialect+driver",
            th.StringType,
            default="mssql+pymssql",
            description=(
                "Dialect+driver see "
                + "https://docs.sqlalchemy.org/en/20/core/engines.html. "
                + "Generally just leave this alone. "
                + "Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "default_target_schema",
            th.StringType,
            description="SqlServer schema to send data to, example: tap-clickup",
            default="melty",
        ),
        th.Property(
            "activate_version",
            th.BooleanType,
            default=True,
            description=(
                "If set to false, the tap will ignore activate version messages. If "
                + "set to true, add_record_metadata must be set to true as well."
            ),
        ),
        th.Property(
            "hard_delete",
            th.BooleanType,
            default=False,
            description=(
                "When activate version is sent from a tap this specefies "
                + "if we should delete the records that don't match, or mark "
                + "them with a date in the `_sdc_deleted_at` column. This config "
                + "option is ignored if `activate_version` is set to false."
            ),
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            default=True,
            description=(
                "Note that this must be enabled for activate_version to work!"
                + "This adds _sdc_extracted_at, _sdc_batched_at, and more to every "
                + "table. See https://sdk.meltano.com/en/latest/implementation/record_metadata.html "  # noqa: E501
                + "for more information."
            ),
        ),
        th.Property(
            "interpret_content_encoding",
            th.BooleanType,
            default=False,
            description=(
                "If set to true, the target will interpret the content encoding of the "
                "schema to determine how to store the data. Using this option may "
                "result in a more efficient storage of the data but may also result "
                "in an error if the data is not encoded as expected."
            ),
        ),
    ).to_dict()
    default_sink_class = SqlServerSink
