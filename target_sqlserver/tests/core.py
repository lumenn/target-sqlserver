"""Config and base values for target-sqlserver testing"""

# flake8: noqa
import sqlalchemy

from target_sqlserver.target import SqlServerTarget


def sqlserver_config():
    return {
        "dialect+driver": "mssql+pymssql",
        "host": "127.0.0.1",
        "user": "sa",
        "password": "VerySecretP455w0rd!",
        "database": "master",
        "port": 1433,
        "add_record_metadata": True,
        "hard_delete": False,
        "default_target_schema": "melty",
        "ssl_enable": False,
        "ssl_client_certificate_enable": False,
    }

def create_engine(target_sqlserver: SqlServerTarget) -> sqlalchemy.engine.Engine:
    return SqlServerTarget.default_sink_class.connector_class(
        config=target_sqlserver.config
    )._engine
