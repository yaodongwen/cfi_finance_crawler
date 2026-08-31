from __future__ import annotations

import pytest

from crawl_framework.storage.repair import (
    StorageRepairAction,
    apply_safe_repairs,
)


class FakeCatalog:

    def __init__(
        self,
    ):

        self.calls = []

    def mark_uploaded(
        self,
        *,
        file_path,
        remote_path,
    ):

        self.calls.append(
            (
                "mark_uploaded",
                file_path,
                remote_path,
            )
        )

    def mark_superseded(
        self,
        *,
        file_path,
    ):

        self.calls.append(
            (
                "mark_superseded",
                file_path,
            )
        )

    def mark_archived(
        self,
        *,
        file_path,
    ):

        self.calls.append(
            (
                "mark_archived",
                file_path,
            )
        )


def test_apply_safe_repairs_dry_run_does_not_mutate_catalog():

    catalog = FakeCatalog()

    actions = [
        StorageRepairAction(
            kind="mark_archived",
            file_path="missing.parquet",
            reason="operator approved archive",
        )
    ]

    result = apply_safe_repairs(
        actions,
        catalog=catalog,
        dry_run=True,
    )

    assert result.dry_run is True
    assert result.planned == 1
    assert result.applied == 0
    assert catalog.calls == []


def test_apply_safe_repairs_runs_explicit_actions_in_order():

    catalog = FakeCatalog()

    actions = [
        StorageRepairAction(
            kind="mark_uploaded",
            file_path="new.parquet",
            remote_path="/remote/new.parquet",
        ),
        StorageRepairAction(
            kind="mark_superseded",
            file_path="old.parquet",
        ),
        StorageRepairAction(
            kind="mark_archived",
            file_path="broken.parquet",
        ),
    ]

    result = apply_safe_repairs(
        actions,
        catalog=catalog,
        dry_run=False,
    )

    assert result.dry_run is False
    assert result.planned == 3
    assert result.applied == 3

    assert catalog.calls == [
        (
            "mark_uploaded",
            "new.parquet",
            "/remote/new.parquet",
        ),
        (
            "mark_superseded",
            "old.parquet",
        ),
        (
            "mark_archived",
            "broken.parquet",
        ),
    ]


def test_mark_uploaded_repair_requires_remote_path():

    with pytest.raises(
        ValueError,
        match="mark_uploaded repair requires remote_path",
    ):

        apply_safe_repairs(
            [
                StorageRepairAction(
                    kind="mark_uploaded",
                    file_path="new.parquet",
                )
            ],
            catalog=FakeCatalog(),
        )


def test_repair_action_rejects_empty_file_path():

    with pytest.raises(
        ValueError,
        match="repair action file_path cannot be empty",
    ):

        apply_safe_repairs(
            [
                StorageRepairAction(
                    kind="mark_archived",
                    file_path=" ",
                )
            ],
            catalog=FakeCatalog(),
        )
