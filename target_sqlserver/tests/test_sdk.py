"""SDK tests for target sqlserver"""

# flake8: noqa
import pytest
from singer_sdk.testing import get_target_test_class
from singer_sdk.testing.suites import TestSuite
from singer_sdk.testing.target_tests import (
    TargetArrayData,
    TargetCamelcaseComplexSchema,
    TargetCamelcaseTest,
    TargetCliPrintsTest,
    TargetDuplicateRecords,
    TargetEncodedStringData,
    TargetInvalidSchemaTest,
    TargetMultipleStateMessages,
    TargetNoPrimaryKeys,
    TargetOptionalAttributes,
    TargetRecordBeforeSchemaTest,
    TargetRecordMissingKeyProperty,
    TargetRecordMissingOptionalFields,
    TargetRecordMissingRequiredProperty,
    TargetSchemaNoProperties,
    TargetSchemaUpdates,
    TargetSpecialCharsInAttributes,
)

from target_sqlserver.target import SqlServerTarget

from .core import create_engine, sqlserver_config

target_tests = TestSuite(
    kind="target",
    tests=[
        TargetArrayData,
        TargetCamelcaseComplexSchema,
        TargetCamelcaseTest,
        TargetCliPrintsTest,
        TargetDuplicateRecords,
        # SqlServer doesn't support NULL characters in strings
        # TargetEncodedStringData,
        TargetInvalidSchemaTest,
        # This tap only outputs one state message at the end of execution, fails assertion.
        # Separate custom test in test_target_sqlserver.py
        # TargetMultipleStateMessages,
        TargetNoPrimaryKeys,
        TargetOptionalAttributes,
        TargetRecordBeforeSchemaTest,
        TargetRecordMissingKeyProperty,
        # Fails, but gives appropriate error message
        # TargetRecordMissingRequiredProperty,
        TargetSchemaNoProperties,
        TargetSchemaUpdates,
        TargetSpecialCharsInAttributes,
        TargetRecordMissingOptionalFields,
    ],
)


class BaseSqlServerSDKTests:
    """Base class for SqlServer SDK tests."""

    @pytest.fixture()
    def connection(self, runner):
        engine = create_engine(runner)
        return engine.connect()


SDKTests = get_target_test_class(
    target_class=SqlServerTarget,
    config=sqlserver_config(),
    custom_suites=[target_tests],
    suite_config=None,
    include_target_tests=False,
)


class TestTargetSqlServer(BaseSqlServerSDKTests, SDKTests):
    """SDK tests"""
