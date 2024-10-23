"""Handles SqlServer interactions."""

from __future__ import annotations

import atexit
import io
import itertools
import signal
import sys
import typing as t
from contextlib import contextmanager
from functools import cached_property
from os import chmod, path
from typing import cast

import paramiko
import simplejson
import sqlalchemy as sa
from sqlalchemy import types
from singer_sdk import SQLConnector
from singer_sdk import typing as th
from sqlalchemy.dialects.mssql import BIGINT, VARBINARY, JSON, UNIQUEIDENTIFIER, NVARCHAR, BIT
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url
from sqlalchemy.types import (
    ARRAY,
    DATE,
    DATETIME,
    DECIMAL,
    INTEGER,
    TIME,
    TypeDecorator,
)
from sshtunnel import SSHTunnelForwarder

if t.TYPE_CHECKING:
    from singer_sdk.connectors.sql import FullyQualifiedName


class SqlServerConnector(SQLConnector):
    """Sets up SQL Alchemy, and other SqlServer related stuff."""

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def __init__(self, config: dict) -> None:
        """Initialize a connector to a SqlServer database.

        Args:
            config: Configuration for the connector.
        """
        url: URL = make_url(self.get_sqlalchemy_url(config=config))

        super().__init__(
            config,
            sqlalchemy_url=url.render_as_string(hide_password=False),
        )

    @cached_property
    def interpret_content_encoding(self) -> bool:
        """Whether to interpret schema contentEncoding to set the column type.

        It is an opt-in feature because it might result in data loss if the
        actual data does not match the schema's advertised encoding.

        Returns:
            True if the feature is enabled, False otherwise.
        """
        return self.config.get("interpret_content_encoding", False)

    def prepare_table(  # type: ignore[override]  # noqa: PLR0913
        self,
        full_table_name: str | FullyQualifiedName,
        schema: dict,
        primary_keys: t.Sequence[str],
        connection: sa.engine.Connection,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> sa.Table:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the target table name.
            schema: the JSON Schema for the table.
            primary_keys: list of key properties.
            connection: the database connection.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Returns:
            The table object.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sa.MetaData(schema=schema_name)
        table: sa.Table
        if not self.table_exists(full_table_name=full_table_name):
            return self.create_empty_table(
                table_name=table_name,
                meta=meta,
                schema=schema,
                primary_keys=primary_keys,
                partition_keys=partition_keys,
                as_temp_table=as_temp_table,
                connection=connection,
            )
        meta.reflect(connection, only=[table_name])
        table = meta.tables[
            full_table_name
        ]  # So we don't mess up the casing of the Table reference

        columns = self.get_table_columns(
            schema_name=cast(str, schema_name),
            table_name=table_name,
            connection=connection,
        )

        for property_name, property_def in schema["properties"].items():
            column_object = None
            if property_name in columns:
                column_object = columns[property_name]
            is_primary_key = property_name in primary_keys
            self.prepare_column(
                full_table_name=table.fullname,
                column_name=property_name,
                sql_type=self.to_sql_type(property_def, is_primary_key),
                connection=connection,
                column_object=column_object,
            )

        return meta.tables[full_table_name]

    def copy_table_structure(
        self,
        full_table_name: str | FullyQualifiedName,
        from_table: sa.Table,
        connection: sa.engine.Connection,
        as_temp_table: bool = False,
    ) -> sa.Table:
        """Copy table structure.

        Args:
            full_table_name: the target table name potentially including schema
            from_table: the  source table
            connection: the database connection.
            as_temp_table: True to create a temp table.

        Returns:
            The new table object.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        meta = sa.MetaData(schema=schema_name)
        new_table: sa.Table
        if self.table_exists(full_table_name=full_table_name):
            raise RuntimeError("Table already exists")

        columns = [column._copy() for column in from_table.columns]
        if as_temp_table:
            new_table = sa.Table(f"#{table_name}", meta, *columns)
            new_table.create(bind=connection)
            return new_table
        new_table = sa.Table(table_name, meta, *columns)
        new_table.create(bind=connection)
        return new_table

    @contextmanager
    def _connect(self) -> t.Iterator[sa.engine.Connection]:
        with self._engine.connect().execution_options() as conn:
            yield conn

    def drop_table(self, table: sa.Table, connection: sa.engine.Connection):
        """Drop table data."""
        table.drop(bind=connection)

    def clone_table(
        self, new_table_name, table, metadata, connection, temp_table
    ) -> sa.Table:
        """Clone a table."""
        new_columns = [sa.Column(column.name, column.type) for column in table.columns]
        if temp_table is True:
            new_table = sa.Table(
                f"#{new_table_name}", metadata, *new_columns 
            )
        else:
            new_table = sa.Table(new_table_name, metadata, *new_columns)
        new_table.create(bind=connection)
        return new_table

    def to_sql_type(self, jsonschema_type: dict, is_primary_key) -> sa.types.TypeEngine:  # type: ignore[override]
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.
        If overriding this method, developers should call the default implementation
        from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        json_type_array = []

        if jsonschema_type.get("type", False):
            if isinstance(jsonschema_type["type"], str):
                json_type_array.append(jsonschema_type)
            elif isinstance(jsonschema_type["type"], list):
                for entry in jsonschema_type["type"]:
                    json_type_dict = {"type": entry}
                    if jsonschema_type.get("format", False):
                        json_type_dict["format"] = jsonschema_type["format"]
                    if encoding := jsonschema_type.get("contentEncoding", False):
                        json_type_dict["contentEncoding"] = encoding
                    # Figure out array type, but only if there's a single type
                    # (no array union types)
                    if (
                        "items" in jsonschema_type
                        and "type" in jsonschema_type["items"]
                        and isinstance(jsonschema_type["items"]["type"], str)
                    ):
                        json_type_dict["items"] = jsonschema_type["items"]["type"]
                    json_type_array.append(json_type_dict)
            else:
                msg = "Invalid format for jsonschema type: not str or list."
                raise RuntimeError(msg)
        elif jsonschema_type.get("anyOf", False):
            json_type_array.extend(iter(jsonschema_type["anyOf"]))
        else:
            msg = (
                "Neither type nor anyOf are present. Unable to determine type. "
                "Defaulting to string."
            )
            return NOTYPE()
        sql_type_array = []
        for json_type in json_type_array:
            picked_type = self.pick_individual_type(jsonschema_type=json_type, is_primary_key=is_primary_key)
            if picked_type is not None:
                sql_type_array.append(picked_type)

        return SqlServerConnector.pick_best_sql_type(sql_type_array=sql_type_array, is_primary_key=is_primary_key)

    def pick_individual_type(self, jsonschema_type: dict, is_primary_key: bool):  # noqa: PLR0911
        """Select the correct sql type assuming jsonschema_type has only a single type.

        Args:
            jsonschema_type: A jsonschema_type array containing only a single type.

        Returns:
            An instance of the appropriate SQL type class based on jsonschema_type.
        """
        if "null" in jsonschema_type["type"]:
            return None
        if "integer" in jsonschema_type["type"]:
            return BIGINT()
        if "object" in jsonschema_type["type"]:
            return JSON()
        if "array" in jsonschema_type["type"]:
            return JSON()
        
        # string formats
        if jsonschema_type.get("format") == "date-time":
            return DATETIME()
        if jsonschema_type.get("format") == "uuid":
            return UNIQUEIDENTIFIER()
        if (
            self.interpret_content_encoding
            and jsonschema_type.get("contentEncoding") == "base16"
        ):
            return HexByteString()
        individual_type = th.to_sql_type(jsonschema_type)

        if isinstance(individual_type, NVARCHAR):
            return SqlServerConnector.select_nvarchar_size(is_primary_key)
        else:
            return individual_type

    @staticmethod
    def pick_best_sql_type(sql_type_array: list, is_primary_key: bool):
        """Select the best SQL type from an array of instances of SQL type classes.

        Args:
            sql_type_array: The array of instances of SQL type classes.

        Returns:
            An instance of the best SQL type class based on defined precedence order.
        """
        precedence_order = [
            HexByteString,
            JSON,
            UNIQUEIDENTIFIER,
            NVARCHAR,
            DATETIME,
            DATE,
            TIME,
            DECIMAL,
            BIGINT,
            INTEGER,
            BIT,
            NOTYPE,
        ]

        for sql_type, obj in itertools.product(precedence_order, sql_type_array):
            if isinstance(obj, sql_type):
                return obj
        return SqlServerConnector.select_nvarchar_size(is_primary_key)

    @staticmethod
    def select_nvarchar_size(is_primary_key: bool) -> NVARCHAR:
        # Max size of clustered index in SQL Server id 900 bytes
        # 450 byte pairs of nvarchar -> https://learn.microsoft.com/en-us/sql/sql-server/maximum-capacity-specifications-for-sql-server?view=sql-server-ver16#-objects
        if is_primary_key:
            return NVARCHAR(450)
        else:
            return NVARCHAR()

    def create_empty_table(  # type: ignore[override]  # noqa: PLR0913
        self,
        table_name: str,
        meta: sa.MetaData,
        schema: dict,
        connection: sa.engine.Connection,
        primary_keys: t.Sequence[str] | None = None,
        partition_keys: list[str] | None = None,
        as_temp_table: bool = False,
    ) -> sa.Table:
        """Create an empty target table.

        Args:
            table_name: the target table name.
            meta: the SQLAlchemy metadata object.
            schema: the JSON schema for the new table.
            connection: the database connection.
            primary_keys: list of key properties.
            partition_keys: list of partition keys.
            as_temp_table: True to create a temp table.

        Returns:
            The new table object.

        Raises:
            NotImplementedError: if temp tables are unsupported and as_temp_table=True.
            RuntimeError: if a variant schema is passed with no properties defined.
        """
        columns: list[sa.Column] = []
        primary_keys = primary_keys or []
        try:
            properties: dict = schema["properties"]
        except KeyError:
            raise RuntimeError(
                f"Schema for table_name: '{table_name}'"
                f"does not define properties: {schema}"
            ) from None

        for property_name, property_jsonschema in properties.items():
            is_primary_key = property_name in primary_keys
            columns.append(
                sa.Column(
                    property_name,
                    self.to_sql_type(property_jsonschema, is_primary_key),
                    primary_key=is_primary_key,
                    autoincrement=False,  # See: https://github.com/MeltanoLabs/target-postgres/issues/193 # noqa: E501
                )
            )
        if as_temp_table:
            new_table = sa.Table(f"#{table_name}", meta, *columns)
        else:
            new_table = sa.Table(table_name, meta, *columns)

        new_table.create(bind=connection)
        return new_table

    def prepare_column(
        self,
        full_table_name: str | FullyQualifiedName,
        column_name: str,
        sql_type: types.TypeEngine,
        connection: sa.engine.Connection | None = None,
        column_object: sa.Column | None = None,
    ) -> None:
        """Adapt target table to provided schema if possible.

        Args:
            full_table_name: the fully qualified table name.
            column_name: the target column name.
            sql_type: the SQLAlchemy type.
            connection: a database connection. optional.
            column_object: a SQLAlchemy column. optional.
        """
        if connection is None:
            super().prepare_column(full_table_name, column_name, sql_type)
            return

        _, schema_name, table_name = self.parse_full_table_name(full_table_name)

        column_exists = column_object is not None or self.column_exists(
            full_table_name, column_name, connection=connection
        )

        if not column_exists:
            self._create_empty_column(
                # We should migrate every function to use sa.Table
                # instead of having to know what the function wants
                table_name=table_name,
                column_name=column_name,
                sql_type=sql_type,
                schema_name=cast(str, schema_name),
                connection=connection,
            )
            return

        self._adapt_column_type(
            schema_name=cast(str, schema_name),
            table_name=table_name,
            column_name=column_name,
            sql_type=sql_type,
            connection=connection,
            column_object=column_object,
        )

    def _create_empty_column(  # type: ignore[override]
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        sql_type: types.TypeEngine,
        connection: sa.engine.Connection,
    ) -> None:
        """Create a new column.

        Args:
            schema_name: The schema name.
            table_name: The table name.
            column_name: The name of the new column.
            sql_type: SQLAlchemy type engine to be used in creating the new column.
            connection: The database connection.

        Raises:
            NotImplementedError: if adding columns is not supported.
        """
        if not self.allow_column_add:
            msg = "Adding columns is not supported."
            raise NotImplementedError(msg)

        column_add_ddl = self.get_column_add_ddl(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            column_type=sql_type,
        )
        connection.execute(column_add_ddl)

    def get_column_add_ddl(  # type: ignore[override]
        self,
        table_name: str,
        schema_name: str,
        column_name: str,
        column_type: types.TypeEngine,
    ) -> sa.DDL:
        """Get the create column DDL statement.

        Args:
            table_name: Fully qualified table name of column to alter.
            schema_name: Schema name.
            column_name: Column name to create.
            column_type: New column sqlalchemy type.

        Returns:
            A sqlalchemy DDL instance.
        """
        column = sa.Column(column_name, column_type)

        return sa.DDL(
            (
                'ALTER TABLE "%(schema_name)s"."%(table_name)s" '
                "ADD %(column_name)s %(column_type)s"
            ),
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column.compile(dialect=self._engine.dialect),
                "column_type": column.type.compile(dialect=self._engine.dialect),
            },
        )

    def _adapt_column_type(  # type: ignore[override]  # noqa: PLR0913
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        sql_type: types.TypeEngine,
        connection: sa.engine.Connection,
        column_object: sa.Column | None,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.

        Args:
            schema_name: The schema name.
            table_name: The table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.
            connection: The database connection.
            column_object: The existing column object.

        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: types.TypeEngine
        if column_object is not None:
            current_type = t.cast(types.TypeEngine, column_object.type)
        else:
            current_type = self._get_column_type(
                schema_name=schema_name,
                table_name=table_name,
                column_name=column_name,
                connection=connection,
            )

        # remove collation if present and save it
        current_type_collation = self.remove_collation(current_type)

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistance
        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type) == str(current_type):
            # Nothing to do
            return

        # Put the collation level back before altering the column
        if current_type_collation:
            self.update_collation(compatible_sql_type, current_type_collation)

        if not self.allow_column_alter:
            msg = (
                "Altering columns is not supported. Could not convert column "
                f"'{schema_name}.{table_name}.{column_name}' from '{current_type}' to "
                f"'{compatible_sql_type}'."
            )
            raise NotImplementedError(msg)

        alter_column_ddl = self.get_column_alter_ddl(
            schema_name=schema_name,
            table_name=table_name,
            column_name=column_name,
            column_type=compatible_sql_type,
        )
        connection.execute(alter_column_ddl)

    def get_column_alter_ddl(  # type: ignore[override]
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        column_type: types.TypeEngine,
    ) -> sa.DDL:
        """Get the alter column DDL statement.

        Override this if your database uses a different syntax for altering columns.

        Args:
            schema_name: Schema name.
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to alter.
            column_type: New column type string.

        Returns:
            A sqlalchemy DDL instance.
        """
        column = sa.Column(column_name, column_type)
        return sa.DDL(
            (
                'ALTER TABLE "%(schema_name)s"."%(table_name)s" '
                "ALTER %(column_name)s %(column_type)s"
            ),
            {
                "schema_name": schema_name,
                "table_name": table_name,
                "column_name": column.compile(dialect=self._engine.dialect),
                "column_type": column.type.compile(dialect=self._engine.dialect),
            },
        )

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generate a SQLAlchemy URL.

        Args:
            config: The configuration for the connector.
        """
        if config.get("sqlalchemy_url"):
            return cast(str, config["sqlalchemy_url"])

        sqlalchemy_url = URL.create(
            drivername=config["dialect+driver"],
            username=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
        )
        return cast(str, sqlalchemy_url)

    def catch_signal(self, signum, frame) -> None:
        """Catch signals and exit cleanly.

        Args:
            signum: The signal number
            frame: The current stack frame
        """
        sys.exit(1)  # Calling this to be sure atexit is called, so clean_up gets called

    def _get_column_type(  # type: ignore[override]
        self,
        schema_name: str,
        table_name: str,
        column_name: str,
        connection: sa.engine.Connection,
    ) -> types.TypeEngine:
        """Get the SQL type of the declared column.

        Args:
            schema_name: The schema name.
            table_name: The table name.
            column_name: The name of the column.
            connection: The database connection.

        Returns:
            The type of the column.

        Raises:
            KeyError: If the provided column name does not exist.
        """
        try:
            column = self.get_table_columns(
                schema_name=schema_name,
                table_name=table_name,
                connection=connection,
            )[column_name]
        except KeyError as ex:
            msg = (
                f"Column `{column_name}` does not exist in table"
                "`{schema_name}.{table_name}`."
            )
            raise KeyError(msg) from ex

        return t.cast(types.TypeEngine, column.type)

    def get_table_columns(  # type: ignore[override]
        self,
        schema_name: str,
        table_name: str,
        connection: sa.engine.Connection,
        column_names: list[str] | None = None,
    ) -> dict[str, sa.Column]:
        """Return a list of table columns.

        Overrode to support schema_name

        Args:
            schema_name: schema name.
            table_name: table name to get columns for.
            connection: database connection.
            column_names: A list of column names to filter to.

        Returns:
            An ordered list of column objects.
        """
        inspector = sa.inspect(connection)
        columns = inspector.get_columns(table_name, schema_name)

        return {
            col_meta["name"]: sa.Column(
                col_meta["name"],
                col_meta["type"],
                nullable=col_meta.get("nullable", False),
            )
            for col_meta in columns
            if not column_names
            or col_meta["name"].casefold() in {col.casefold() for col in column_names}
        }

    def column_exists(  # type: ignore[override]
        self,
        full_table_name: str | FullyQualifiedName,
        column_name: str,
        connection: sa.engine.Connection,
    ) -> bool:
        """Determine if the target column already exists.

        Args:
            full_table_name: the target table name.
            column_name: the target column name.
            connection: the database connection.

        Returns:
            True if table exists, False if not.
        """
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        assert schema_name is not None
        assert table_name is not None
        return column_name in self.get_table_columns(
            schema_name=schema_name, table_name=table_name, connection=connection
        )


class NOTYPE(TypeDecorator):
    """Type to use when none is provided in the schema."""

    impl = NVARCHAR
    cache_ok = True

    def process_bind_param(self, value, dialect):
        """Return value as is unless it is dict or list.

        Used internally by SQL Alchemy. Should not be used directly.
        """
        if value is not None and isinstance(value, (dict, list)):
            value = simplejson.dumps(value, use_decimal=True)
        return value

    @property
    def python_type(self):
        """Return the Python type for this column."""
        return object

    def as_generic(self, *args: t.Any, **kwargs: t.Any):
        """Return the generic type for this column."""
        return NVARCHAR


class HexByteString(TypeDecorator):
    """Convert Python string representing Hex data to bytes and vice versa.

    This is used to store binary data in more efficient format in the database.
    The string is encoded using the base16 encoding, as defined in RFC 4648
    https://json-schema.org/draft/2020-12/draft-bhutton-json-schema-validation-00#rfc.section.8.3
    For convenience, data prefixed with `0x` or containing an odd number of characters
    is supported although it's not part of the standard.
    """

    impl = VARBINARY

    def process_bind_param(self, value, dialect):
        """Convert hex string to bytes."""
        if value is None:
            return None

        if isinstance(value, str):
            if value.startswith("\\x") or value.startswith("0x"):
                value = value[2:]

            if len(value) % 2:
                value = f"0{value}"

            try:
                value = bytes.fromhex(value)
            except ValueError as ex:
                raise ValueError(f"Invalid hexadecimal string: {value}") from ex

        if not isinstance(value, (bytearray, memoryview, bytes)):
            raise TypeError(
                "HexByteString columns support only bytes or hex string values. "
                f"{type(value)} is not supported"
            )

        return value
