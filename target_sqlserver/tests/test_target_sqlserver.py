"""SqlServer target tests"""

from __future__ import annotations

# flake8: noqa
import copy
import io
import json
from contextlib import redirect_stdout
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy
from urllib.parse import quote_plus
from sqlalchemy import exc
from singer_sdk.exceptions import InvalidRecord, MissingKeyPropertiesError
from singer_sdk.testing import sync_end_to_end
from sqlalchemy.dialects.mssql import NVARCHAR, DATETIME

from target_sqlserver.connector import SqlServerConnector
from target_sqlserver.target import SqlServerTarget
from target_sqlserver.tests.samples.aapl.aapl import Fundamentals
from target_sqlserver.tests.samples.sample_tap_countries.countries_tap import (
    SampleTapCountries,
)

from .core import (
    create_engine,
    sqlserver_config,
)

# The below syntax is documented at https://docs.pytest.org/en/stable/deprecations.html#calling-fixtures-directly
@pytest.fixture(scope="session", name="sqlserver_config")
def sqlserver_config_fixture():
    return sqlserver_config()

@pytest.fixture
def sqlserver_target(sqlserver_config) -> SqlServerTarget:
    return SqlServerTarget(config=sqlserver_config)

def singer_file_to_target(file_name, target) -> None:
    """Singer file to Target, emulates a tap run

    Equivalent to running cat file_path | target-name --config config.json.
    Note that this function loads all lines into memory, so it is
    not good very large files.

    Args:
        file_name: name to file in .tests/data_files to be sent into target
        Target: Target to pass data from file_path into..
    """
    file_path = Path(__file__).parent / Path("./data_files") / Path(file_name)
    buf = io.StringIO()
    with redirect_stdout(buf):
        with open(file_path) as f:
            for line in f:
                print(line.rstrip("\r\n"))  # File endings are here,
                # and print adds another line ending so we need to remove one.
    buf.seek(0)
    target.listen(buf)


# TODO should set schemas for each tap individually so we don't collide


def remove_metadata_columns(row: dict) -> dict:
    new_row = {}
    for column in row.keys():
        if not column.startswith("_sdc"):
            new_row[column] = row[column]
    return new_row


def verify_data(
    target: SqlServerTarget,
    table_name: str,
    number_of_rows: int = 1,
    primary_key: str | None = None,
    check_data: dict | list[dict] | None = None,
):
    """Checks whether the data in a table matches a provided data sample.

    Args:
        target: The target to obtain a database connection from.
        full_table_name: The schema and table name of the table to check data for.
        primary_key: The primary key of the table.
        number_of_rows: The expected number of rows that should be in the table.
        check_data: A dictionary representing the full contents of the first row in the
            table, as determined by lowest primary_key value, or else a list of
            dictionaries representing every row in the table.
    """
    engine = create_engine(target)
    full_table_name = f"{target.config['default_target_schema']}.{table_name}"
    with engine.connect() as connection:
        if primary_key is not None and check_data is not None:
            if isinstance(check_data, dict):
                result = connection.execute(
                    sqlalchemy.text(
                        f"SELECT * FROM {full_table_name} ORDER BY {primary_key}"
                    )
                ).fetchall()

                assert len(result) == number_of_rows
                data = convert_to_dict(result[0])
                assert remove_metadata_columns(data) == check_data
            elif isinstance(check_data, list):
                result = connection.execute(
                    sqlalchemy.text(
                        f"SELECT * FROM {full_table_name} ORDER BY {primary_key}"
                    )
                ).fetchall()
                assert len(result) == number_of_rows
                result_dict = [convert_to_dict(row) for row in result]
                # bytea columns are returned as memoryview objects
                # we need to convert them to bytes to allow comparison with check_data
                for row in result_dict:
                    for col in row:
                        if isinstance(row[col], memoryview):
                            row[col] = bytes(row[col])

                assert result_dict == check_data
            else:
                raise ValueError("Invalid check_data - not dict or list of dicts")
        else:
            result = connection.execute(
                sqlalchemy.text(f"SELECT COUNT(*) FROM {full_table_name}")
            )
            assert result.first()[0] == number_of_rows

def convert_to_dict(row: sqlalchemy.engine.row.Row):
    dict_row = row._asdict()
    new_dict_row: dict = {}
    for column in dict_row.keys():
        try:
            dict_column = json.loads(dict_row[column])
        except:
            dict_column = dict_row[column]

        new_dict_row[column] = dict_column
        
    return remove_metadata_columns(new_dict_row)

def test_sqlalchemy_url_config(sqlserver_config):
    """Be sure that passing a sqlalchemy_url works

    sqlserver_config is used because an SQLAlchemy URL will override all SSL
    settings and preclude connecting to a database using SSL.
    """
    host = sqlserver_config["host"]
    user = sqlserver_config["user"]
    password = sqlserver_config["password"]
    database = sqlserver_config["database"]
    port = sqlserver_config["port"]

    config = {
        "sqlalchemy_url": f"mssql+pymssql://{user}:{quote_plus(password)}@{host}:{port}/{database}"
    }
    tap = SampleTapCountries(config={}, state=None)
    target = SqlServerTarget(config=config)
    sync_end_to_end(tap, target)


def test_port_default_config():
    """Test that the default config is passed into the engine when the config doesn't provide it"""
    config = {
        "dialect+driver": "mssql+pymssql",
        "host": "127.0.0.1",
        "user": "sa",
        "password": "VerySecretP455w0rd!",
        "database": "master",
    }
    dialect_driver = config["dialect+driver"]
    host = config["host"]
    user = config["user"]
    password = config["password"]
    database = config["database"]
    target_config = SqlServerTarget(config=config).config
    connector = SqlServerConnector(target_config)

    engine: sqlalchemy.engine.Engine = connector._engine
    assert (
        engine.url.render_as_string(hide_password=False)
        == f"{dialect_driver}://{user}:{quote_plus(password)}@{host}:1433/{database}"
    )


def test_port_config():
    """Test that the port config works"""
    config = {
        "dialect+driver": "mssql+pymssql",
        "host": "127.0.0.1",
        "user": "sa",
        "password": "VerySecretP455w0rd!",
        "database": "master",
        "port": 1434,
    }
    dialect_driver = config["dialect+driver"]
    host = config["host"]
    user = config["user"]
    password = config["password"]
    database = config["database"]
    target_config = SqlServerTarget(config=config).config
    connector = SqlServerConnector(target_config)

    engine: sqlalchemy.engine.Engine = connector._engine
    assert (
        engine.url.render_as_string(hide_password=False)
        == f"{dialect_driver}://{user}:{quote_plus(password)}@{host}:1434/{database}"
    )


# Test name would work well
def test_countries_to_sqlserver(sqlserver_config):
    tap = SampleTapCountries(config={}, state=None)
    target = SqlServerTarget(config=sqlserver_config)
    sync_end_to_end(tap, target)


def test_aapl_to_sqlserver(sqlserver_config):
    tap = Fundamentals(config={}, state=None)
    target = SqlServerTarget(config=sqlserver_config)
    sync_end_to_end(tap, target)


def test_invalid_schema(sqlserver_target):
    with pytest.raises(Exception) as e:
        file_name = "invalid_schema.singer"
        singer_file_to_target(file_name, sqlserver_target)
    assert (
        str(e.value) == "Line is missing required properties key(s): {'type': 'object'}"
    )


def test_record_missing_key_property(sqlserver_target):
    with pytest.raises(MissingKeyPropertiesError) as e:
        file_name = "record_missing_key_property.singer"
        singer_file_to_target(file_name, sqlserver_target)
    assert "Record is missing one or more key_properties." in str(e.value)


def test_record_missing_required_property(sqlserver_target):
    with pytest.raises(InvalidRecord):
        file_name = "record_missing_required_property.singer"
        singer_file_to_target(file_name, sqlserver_target)


def test_camelcase(sqlserver_target):
    file_name = "camelcase.singer"
    singer_file_to_target(file_name, sqlserver_target)


def test_special_chars_in_attributes(sqlserver_target):
    file_name = "special_chars_in_attributes.singer"
    singer_file_to_target(file_name, sqlserver_target)


def test_optional_attributes(sqlserver_target):
    file_name = "optional_attributes.singer"
    singer_file_to_target(file_name, sqlserver_target)
    row = {"id": 1, "optional": "This is optional"}
    verify_data(sqlserver_target, "test_optional_attributes", 4, "id", row)


def test_schema_no_properties(sqlserver_target):
    """Expect to fail with ValueError"""
    file_name = "schema_no_properties.singer"
    singer_file_to_target(file_name, sqlserver_target)


# TODO test that data is correct
def test_large_numeric_primary_key(sqlserver_target):
    """Check that large numeric (jsonschema: number) pkeys don't cause failure.

    See: https://github.com/MeltanoLabs/target-sqlserver/issues/193
    """
    file_name = "large_numeric_primary_key.singer"
    singer_file_to_target(file_name, sqlserver_target)


# TODO test that data is correct
def test_schema_updates(sqlserver_target):
    file_name = "schema_updates.singer"
    singer_file_to_target(file_name, sqlserver_target)
    row = {
        "id": 1,
        "a1": Decimal("101"),
        "a2": "string1",
        "a3": None,
        "a4": None,
        "a5": None,
        "a6": None,
    }
    verify_data(sqlserver_target, "test_schema_updates", 6, "id", row)


def test_multiple_state_messages(sqlserver_target):
    file_name = "multiple_state_messages.singer"
    singer_file_to_target(file_name, sqlserver_target)
    row = {"id": 1, "metric": 100}
    verify_data(sqlserver_target, "test_multiple_state_messages_a", 6, "id", row)
    row = {"id": 1, "metric": 110}
    verify_data(sqlserver_target, "test_multiple_state_messages_b", 6, "id", row)


# TODO test that data is correct
def test_multiple_schema_messages(sqlserver_target, caplog):
    """Test multiple identical schema messages.

    Multiple schema messages with the same schema should not cause 'schema has changed'
    logging statements. See: https://github.com/MeltanoLabs/target-sqlserver/issues/124

    Caplog docs: https://docs.pytest.org/en/latest/how-to/logging.html#caplog-fixture
    """
    file_name = "multiple_schema_messages.singer"
    singer_file_to_target(file_name, sqlserver_target)
    assert "Schema has changed for stream" not in caplog.text


def test_relational_data(sqlserver_target):
    file_name = "user_location_data.singer"
    singer_file_to_target(file_name, sqlserver_target)

    file_name = "user_location_upsert_data.singer"
    singer_file_to_target(file_name, sqlserver_target)

    users = [
        {"id": 1, "name": "Johny"},
        {"id": 2, "name": "George"},
        {"id": 3, "name": "Jacob"},
        {"id": 4, "name": "Josh"},
        {"id": 5, "name": "Jim"},
        {"id": 8, "name": "Thomas"},
        {"id": 12, "name": "Paul"},
        {"id": 13, "name": "Mary"},
    ]
    locations = [
        {"id": 1, "name": "Philly"},
        {"id": 2, "name": "NY"},
        {"id": 3, "name": "San Francisco"},
        {"id": 6, "name": "Colorado"},
        {"id": 8, "name": "Boston"},
    ]
    user_in_location = [
        {
            "id": 1,
            "user_id": 1,
            "location_id": 4,
            "info": {"weather": "rainy", "mood": "sad"},
        },
        {
            "id": 2,
            "user_id": 2,
            "location_id": 3,
            "info": {"weather": "sunny", "mood": "satisfied"},
        },
        {
            "id": 3,
            "user_id": 1,
            "location_id": 3,
            "info": {"weather": "sunny", "mood": "happy"},
        },
        {
            "id": 6,
            "user_id": 3,
            "location_id": 2,
            "info": {"weather": "sunny", "mood": "happy"},
        },
        {
            "id": 14,
            "user_id": 4,
            "location_id": 1,
            "info": {"weather": "cloudy", "mood": "ok"},
        },
    ]

    verify_data(sqlserver_target, "test_users", 8, "id", users)
    verify_data(sqlserver_target, "test_locations", 5, "id", locations)
    verify_data(sqlserver_target, "test_user_in_location", 5, "id", user_in_location)


def test_no_primary_keys(sqlserver_target):
    """We run both of these tests twice just to ensure that no records are removed and append only works properly"""
    engine = create_engine(sqlserver_target)
    table_name = "test_no_pk"
    full_table_name = sqlserver_target.config["default_target_schema"] + "." + table_name
    with engine.connect() as connection, connection.begin():
        connection.execute(sqlalchemy.text(f"DROP TABLE IF EXISTS {full_table_name}"))
    file_name = f"{table_name}.singer"
    singer_file_to_target(file_name, sqlserver_target)

    file_name = f"{table_name}_append.singer"
    singer_file_to_target(file_name, sqlserver_target)

    file_name = f"{table_name}.singer"
    singer_file_to_target(file_name, sqlserver_target)

    file_name = f"{table_name}_append.singer"
    singer_file_to_target(file_name, sqlserver_target)

    verify_data(sqlserver_target, table_name, 16)


def test_no_type(sqlserver_target):
    file_name = "test_no_type.singer"
    singer_file_to_target(file_name, sqlserver_target)


def test_duplicate_records(sqlserver_target):
    file_name = "duplicate_records.singer"
    singer_file_to_target(file_name, sqlserver_target)
    row = {"id": 1, "metric": 100}
    verify_data(sqlserver_target, "test_duplicate_records", 2, "id", row)


def test_array_data(sqlserver_target):
    file_name = "array_data.singer"
    singer_file_to_target(file_name, sqlserver_target)
    row = {"id": 1, "fruits": ["apple", "orange", "pear"]}
    verify_data(sqlserver_target, "test_carts", 4, "id", row)


def test_jsonb_data(sqlserver_target):
    file_name = "jsonb_data.singer"
    singer_file_to_target(file_name, sqlserver_target)
    row = [
        {"id": 1, "event_data": None},
        {"id": 2, "event_data": {"test": {"test_name": "test_value"}}},
    ]
    verify_data(sqlserver_target, "test_jsonb_data", 2, "id", row)


def test_encoded_string_data(sqlserver_target):
    file_name = "encoded_strings.singer"
    singer_file_to_target(file_name, sqlserver_target)
    row = {"id": 1, "info": "simple string 2837"}
    verify_data(sqlserver_target, "test_strings", 11, "id", row)
    row = {"id": 1, "info": {"name": "simple", "value": "simple string 2837"}}
    verify_data(sqlserver_target, "test_strings_in_objects", 11, "id", row)
    row = {"id": 1, "strings": ["simple string", "απλή συμβολοσειρά", "简单的字串"]}
    verify_data(sqlserver_target, "test_strings_in_arrays", 6, "id", row)


def test_tap_appl(sqlserver_target):
    """Expect to fail with ValueError due to primary key https://github.com/MeltanoLabs/target-postgres/issues/54"""
    file_name = "tap_aapl.singer"
    singer_file_to_target(file_name, sqlserver_target)


def test_tap_countries(sqlserver_target):
    file_name = "tap_countries.singer"
    singer_file_to_target(file_name, sqlserver_target)


def test_missing_value(sqlserver_target):
    file_name = "missing_value.singer"
    singer_file_to_target(file_name, sqlserver_target)


def test_large_int(sqlserver_target):
    file_name = "large_int.singer"
    singer_file_to_target(file_name, sqlserver_target)


def test_anyof(sqlserver_target):
    """Test that anyOf is handled correctly"""
    engine = create_engine(sqlserver_target)
    table_name = "commits"
    file_name = f"{table_name}.singer"
    schema = sqlserver_target.config["default_target_schema"]
    singer_file_to_target(file_name, sqlserver_target)
    with engine.connect() as connection:
        meta = sqlalchemy.MetaData()
        table = sqlalchemy.Table(
            "commits", meta, schema=schema, autoload_with=connection
        )
        for column in table.c:
            # {"type":"string"}
            if column.name == "id":
                assert isinstance(column.type, NVARCHAR)

            # Any of nullable date-time.
            # Note that sqlserver timestamp is equivalent to jsonschema date-time.
            # {"anyOf":[{"type":"string","format":"date-time"},{"type":"null"}]}
            if column.name in {"authored_date", "committed_date"}:
                assert isinstance(column.type, DATETIME)

            # Any of nullable string.
            # {"anyOf":[{"type":"string"},{"type":"null"}]}
            if column.name == "commit_message":
                assert isinstance(column.type, NVARCHAR)

            # Any of nullable string or integer.
            # {"anyOf":[{"type":"string"},{"type":"integer"},{"type":"null"}]}
            if column.name == "legacy_id":
                assert isinstance(column.type, NVARCHAR)


def test_new_array_column(sqlserver_target):
    """Create a new Array column with an existing table"""
    file_name = "new_array_column.singer"
    singer_file_to_target(file_name, sqlserver_target)


def test_base16_content_encoding_not_interpreted(sqlserver_config):
    """Make sure we can insert base16 encoded data into the database without interpretation"""
    sqlserver_config_modified = copy.deepcopy(sqlserver_config)
    sqlserver_config_modified["interpret_content_encoding"] = False
    target = SqlServerTarget(config=sqlserver_config_modified)

    singer_file_to_target("base16_content_encoding_not_interpreted.singer", target)

    rows = [
        {"id": "empty_0x_str", "contract_address": "0x", "raw_event_data": "0x"},
        {"id": "empty_str", "contract_address": "", "raw_event_data": ""},
        {
            "id": "test_handle_an_hex_str",
            "contract_address": "0xA1B2C3D4E5F607080910",
            "raw_event_data": "0xA1B2C3D4E5F60708091001020304050607080910010203040506070809100102030405060708091001020304050607080910",
        },
        {
            "id": "test_handle_hex_without_the_0x_prefix",
            "contract_address": "A1B2C3D4E5F607080910",
            "raw_event_data": "A1B2C3D4E5F6070809100102030405060",
        },
        {
            "id": "test_handle_odd_and_even_number_of_chars",
            "contract_address": "0xA1",
            "raw_event_data": "A12",
        },
        {
            "id": "test_handle_upper_and_lowercase_hex",
            "contract_address": "0xa1",
            "raw_event_data": "A12b",
        },
        {"id": "test_nullable_field", "contract_address": "", "raw_event_data": None},
    ]
    verify_data(target, "test_base_16_encoding_not_interpreted", 7, "id", rows)


def test_base16_content_encoding_interpreted(sqlserver_config):
    """Make sure we can insert base16 encoded data into the database with interpretation"""
    sqlserver_config_modified = copy.deepcopy(sqlserver_config)
    sqlserver_config_modified["interpret_content_encoding"] = True
    target = SqlServerTarget(config=sqlserver_config_modified)

    singer_file_to_target("base16_content_encoding_interpreted.singer", target)

    rows = [
        {"id": "empty_0x_str", "contract_address": b"", "raw_event_data": b""},
        {"id": "empty_str", "contract_address": b"", "raw_event_data": b""},
        {
            "id": "test_handle_an_hex_str",
            "contract_address": b"\xa1\xb2\xc3\xd4\xe5\xf6\x07\x08\x09\x10",
            "raw_event_data": b"\xa1\xb2\xc3\xd4\xe5\xf6\x07\x08\x09\x10\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10\x01\x02\x03\x04\x05\x06\x07\x08\x09\x10",
        },
        {
            "id": "test_handle_hex_without_the_0x_prefix",
            "contract_address": b"\xa1\xb2\xc3\xd4\xe5\xf6\x07\x08\x09\x10",
            "raw_event_data": b"\x0a\x1b\x2c\x3d\x4e\x5f\x60\x70\x80\x91\x00\x10\x20\x30\x40\x50\x60",
        },
        {
            "id": "test_handle_odd_and_even_number_of_chars",
            "contract_address": b"\xa1",
            "raw_event_data": b"\x0a\x12",
        },
        {
            "id": "test_handle_upper_and_lowercase_hex",
            "contract_address": b"\xa1",
            "raw_event_data": b"\xa1\x2b",
        },
        {"id": "test_nullable_field", "contract_address": b"", "raw_event_data": None},
    ]
    verify_data(target, "test_base_16_encoding_interpreted", 7, "id", rows)


def test_activate_version_hard_delete(sqlserver_config):
    """Activate Version Hard Delete Test"""
    table_name = "test_activate_version_hard"
    file_name = f"{table_name}.singer"
    full_table_name = sqlserver_config["default_target_schema"] + "." + table_name
    sqlserver_config_hard_delete_true = copy.deepcopy(sqlserver_config)
    sqlserver_config_hard_delete_true["hard_delete"] = True
    pg_hard_delete_true = SqlServerTarget(config=sqlserver_config_hard_delete_true)
    engine = create_engine(pg_hard_delete_true)
    singer_file_to_target(file_name, pg_hard_delete_true)
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert len(result.fetchall()) == 7
    with engine.connect() as connection, connection.begin():
        # Add a record like someone would if they weren't using the tap target combo
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual1', 'Meltano')"
            )
        )
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual2', 'Meltano')"
            )
        )
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert len(result.fetchall()) == 9

    singer_file_to_target(file_name, pg_hard_delete_true)

    # Should remove the 2 records we added manually
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert len(result.fetchall()) == 7


def test_activate_version_soft_delete(sqlserver_config):
    """Activate Version Soft Delete Test"""
    table_name = "test_activate_version_soft"
    file_name = f"{table_name}.singer"
    full_table_name = sqlserver_config["default_target_schema"] + "." + table_name
    sqlserver_config_hard_delete_false = copy.deepcopy(sqlserver_config)
    sqlserver_config_hard_delete_false["hard_delete"] = False
    pg_soft_delete = SqlServerTarget(config=sqlserver_config_hard_delete_false)
    engine = create_engine(pg_soft_delete)
    singer_file_to_target(file_name, pg_soft_delete)
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert len(result.fetchall()) == 7

    # Same file as above, but with South America (code=SA) record missing.
    file_name = f"{table_name}_with_delete.singer"
    south_america = {}

    singer_file_to_target(file_name, pg_soft_delete)
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert len(result.fetchall()) == 7
        result = connection.execute(
            sqlalchemy.text(f"SELECT * FROM {full_table_name} WHERE code='SA'")
        )
        south_america = result.first()._asdict()

    singer_file_to_target(file_name, pg_soft_delete)
    with engine.connect() as connection, connection.begin():
        # Add a record like someone would if they weren't using the tap target combo
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual1', 'Meltano')"
            )
        )
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name}(code, \"name\") VALUES('Manual2', 'Meltano')"
            )
        )
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert len(result.fetchall()) == 9

    singer_file_to_target(file_name, pg_soft_delete)

    # Should have all records including the 2 we added manually
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert len(result.fetchall()) == 9

        result = connection.execute(
            sqlalchemy.text(
                f"SELECT * FROM {full_table_name} where _sdc_deleted_at is NOT NULL"
            )
        )
        assert len(result.fetchall()) == 3  # 2 manual + 1 deleted (south america)

        result = connection.execute(
            sqlalchemy.text(f"SELECT * FROM {full_table_name} WHERE code='SA'")
        )
        # South America row should not have been modified, but it would have been prior
        # to the fix mentioned in #204 and implemented in #240.
        assert south_america == result.first()._asdict()


def test_activate_version_no_metadata(sqlserver_config):
    """Activate Version Test for if add_record_metadata is disabled"""
    sqlserver_config_modified = copy.deepcopy(sqlserver_config)
    sqlserver_config_modified["activate_version"] = True
    sqlserver_config_modified["add_record_metadata"] = False
    with pytest.raises(AssertionError):
        SqlServerTarget(config=sqlserver_config_modified)


def test_activate_version_deletes_data_properly(sqlserver_target):
    """Activate Version should"""
    engine = create_engine(sqlserver_target)
    table_name = "test_activate_version_deletes_data_properly"
    file_name = f"{table_name}.singer"
    full_table_name = sqlserver_target.config["default_target_schema"] + "." + table_name
    with engine.connect() as connection, connection.begin():
        result = connection.execute(
            sqlalchemy.text(f"DROP TABLE IF EXISTS {full_table_name}")
        )

    sqlserver_config_soft_delete = copy.deepcopy(sqlserver_target._config)
    sqlserver_config_soft_delete["hard_delete"] = True
    pg_hard_delete = SqlServerTarget(config=sqlserver_config_soft_delete)
    singer_file_to_target(file_name, pg_hard_delete)
    # Will populate us with 7 records
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert len(result.fetchall()) == 7
    with engine.connect() as connection, connection.begin():
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name} (code, \"name\") VALUES('Manual1', 'Meltano')"
            )
        )
        result = connection.execute(
            sqlalchemy.text(
                f"INSERT INTO {full_table_name} (code, \"name\") VALUES('Manual2', 'Meltano')"
            )
        )
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert len(result.fetchall()) == 9
    # Only has a schema and one activate_version message, should delete all records as it's a higher version than what's currently in the table
    file_name = f"{table_name}_2.singer"
    singer_file_to_target(file_name, pg_hard_delete)
    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text(f"SELECT * FROM {full_table_name}"))
        assert len(result.fetchall()) == 0


def test_reserved_keywords(sqlserver_target):
    """Target should work regardless of column names

    Postgres has a number of resereved keywords listed here https://www.sqlserverql.org/docs/current/sql-keywords-appendix.html.
    """
    file_name = "reserved_keywords.singer"
    singer_file_to_target(file_name, sqlserver_target)


def test_uppercase_stream_name_with_column_alter(sqlserver_target):
    """Column Alters need to work with uppercase stream names"""
    file_name = "uppercase_stream_name_with_column_alter.singer"
    singer_file_to_target(file_name, sqlserver_target)


def test_activate_version_uppercase_stream_name(sqlserver_config):
    """Activate Version should work with uppercase stream names"""
    file_name = "test_activate_version_uppercase_stream_name.singer"
    sqlserver_config_hard_delete = copy.deepcopy(sqlserver_config)
    sqlserver_config_hard_delete["hard_delete"] = True
    pg_hard_delete = SqlServerTarget(config=sqlserver_config_hard_delete)
    singer_file_to_target(file_name, pg_hard_delete)
