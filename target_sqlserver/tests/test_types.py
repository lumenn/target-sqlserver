"""Test custom types and the type hierarchy."""

import pytest
import sqlalchemy as sa

from target_sqlserver.connector import NOTYPE, SqlServerConnector


@pytest.fixture
def connector():
    """Create a SqlServerConnector instance."""
    return SqlServerConnector(
        config={
            "dialect+driver": "mssql+pymssql",
            "host": "127.0.0.1",
            "port": "1443",
            "user": "sa",
            "password": "VerySecretP455w0rd!",
            "database": "master"
        },
    )


@pytest.mark.parametrize(
    ("types", "expected"),
    [
        pytest.param([sa.Integer(), sa.String()], sa.String, id="int+str=str"),
        pytest.param([sa.Boolean(), sa.String()], sa.String, id="bool+str=str"),
        pytest.param([sa.Integer(), sa.DateTime()], sa.Integer, id="int+datetime=int"),
        pytest.param([NOTYPE(), sa.String()], sa.String, id="none+str=str"),
        pytest.param([NOTYPE(), sa.Integer()], NOTYPE, id="none+int=none"),
    ],
)
def test_type_hierarchy(connector, types, expected):
    """Test that types are merged correctly."""
    assert type(connector.merge_sql_types(types)) is expected
