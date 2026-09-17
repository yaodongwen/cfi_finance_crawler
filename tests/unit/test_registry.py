import pyarrow as pa
import pytest

from crawl_framework.core.dataset import (
    list_datasets,
)
from crawl_framework.core.registry import (
    COMMON_SCHEMA,
    DEFAULT_SCHEMA_REGISTRY,
    DatasetSchema,
    DatasetSchemaRegistry,
    get_dataset_schema,
    list_dataset_schemas,
)


def test_common_schema_fields():

    names = (
        COMMON_SCHEMA.names
    )

    assert (
        "record_uid"
        in names
    )

    assert (
        "version_hash"
        in names
    )

    assert (
        "event_time"
        in names
    )

    assert (
        "payload_json"
        in names
    )


def test_timestamp_types():

    assert pa.types.is_timestamp(
        COMMON_SCHEMA.field(
            "event_time"
        ).type
    )

    assert pa.types.is_timestamp(
        COMMON_SCHEMA.field(
            "crawled_at"
        ).type
    )

    assert (
        COMMON_SCHEMA.field(
            "event_time"
        ).type.tz
        == "UTC"
    )


def test_nullable_fields():

    assert (
        COMMON_SCHEMA.field(
            "scope_id"
        ).nullable
        is True
    )

    assert (
        COMMON_SCHEMA.field(
            "instrument_id"
        ).nullable
        is True
    )

    assert (
        COMMON_SCHEMA.field(
            "record_uid"
        ).nullable
        is False
    )


def test_payload_is_large_string():

    assert pa.types.is_large_string(
        COMMON_SCHEMA.field(
            "payload_json"
        ).type
    )


def test_default_registry_contains_all_datasets():

    logical = set(
        list_datasets()
    )

    physical = set(
        list_dataset_schemas()
    )

    assert (
        logical
        == physical
    )


def test_get_dataset_schema():

    schema = get_dataset_schema(
        "forum_post"
    )

    assert (
        schema.name
        == "forum_post"
    )

    assert (
        schema.spec.name
        == "forum_post"
    )

    assert (
        schema.schema_version
        == 1
    )


@pytest.mark.parametrize(
    "dataset",
    (
        "financial_report",
        "financial_report_instrument",
        "attachment",
    ),
)
def test_financial_report_schemas_use_canonical_contract(dataset):

    schema = get_dataset_schema(dataset)

    assert schema.arrow_schema == COMMON_SCHEMA
    assert schema.arrow_schema.field("payload_json").nullable is False
    assert schema.arrow_schema.field("relations_json").nullable is False


def test_unknown_schema():

    with pytest.raises(
        KeyError
    ):
        get_dataset_schema(
            "not_exists"
        )


def test_registry_duplicate():

    registry = (
        DatasetSchemaRegistry()
    )

    schema = get_dataset_schema(
        "forum_post"
    )

    registry.register(
        schema
    )

    with pytest.raises(
        KeyError
    ):
        registry.register(
            schema
        )


def test_registry_replace():

    registry = (
        DatasetSchemaRegistry()
    )

    original = get_dataset_schema(
        "forum_post"
    )

    registry.register(
        original
    )

    replacement = DatasetSchema(
        name="forum_post",
        spec=original.spec,
        arrow_schema=original.arrow_schema,
        schema_version=2,
    )

    registry.register(
        replacement,
        replace=True,
    )

    assert (
        registry.get(
            "forum_post"
        ).schema_version
        == 2
    )


def test_default_registry_not_empty():

    assert (
        len(
            DEFAULT_SCHEMA_REGISTRY
            .names()
        )
        > 0
    )
