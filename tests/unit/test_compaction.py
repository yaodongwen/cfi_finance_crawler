from __future__ import annotations

from datetime import (
    date,
    datetime,
    timezone,
)
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from crawl_framework.storage.compaction import (
    build_compaction_plan,
    compact_active_dataset,
    publish_compaction_result,
    write_compacted_group,
)
from crawl_framework.storage.postgres import (
    CatalogDataFile,
)
from crawl_framework.storage.uploader import (
    UploadResult,
)


UTC = timezone.utc


def make_table(
    rows,
) -> pa.Table:

    return pa.table(
        {
            "schema_version":
                pa.array(
                    [
                        row["schema_version"]
                        for row in rows
                    ],
                    type=pa.int32(),
                ),
            "record_uid":
                pa.array(
                    [
                        row["record_uid"]
                        for row in rows
                    ],
                    type=pa.string(),
                ),
            "event_time":
                pa.array(
                    [
                        row["event_time"]
                        for row in rows
                    ],
                    type=pa.timestamp(
                        "us",
                        tz="UTC",
                    ),
                ),
            "instrument_id":
                pa.array(
                    [
                        row["instrument_id"]
                        for row in rows
                    ],
                    type=pa.string(),
                ),
        }
    )


def write_parquet(
    path: Path,
    rows,
) -> None:

    pq.write_table(
        make_table(
            rows
        ),
        path,
    )


def make_file(
    *,
    file_id: int,
    path: Path,
    row_count: int,
    storage_status: str = "uploaded",
    lifecycle_status: str = "active",
) -> CatalogDataFile:

    return CatalogDataFile(
        id=file_id,
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        partition_date="2026-08-26",
        bucket="20",
        file_path=path.name,
        sha256="sha",
        row_count=row_count,
        file_size=(
            path.stat().st_size
            if path.exists()
            else 0
        ),
        min_event_time=None,
        max_event_time=None,
        schema_version=1,
        storage_status=storage_status,
        lifecycle_status=lifecycle_status,
        remote_path=str(
            path
        ),
    )


def test_build_compaction_plan_groups_only_active_uploaded_files(
    tmp_path,
):

    active_1 = tmp_path / "active-1.parquet"
    active_2 = tmp_path / "active-2.parquet"
    local_file = tmp_path / "local.parquet"
    superseded = tmp_path / "superseded.parquet"

    for path in (
        active_1,
        active_2,
        local_file,
        superseded,
    ):

        write_parquet(
            path,
            [
                {
                    "schema_version": 1,
                    "record_uid": path.stem,
                    "event_time": datetime(
                        2026,
                        8,
                        26,
                        1,
                        tzinfo=UTC,
                    ),
                    "instrument_id": "XKRX:042700",
                }
            ],
        )

    plan = build_compaction_plan(
        [
            make_file(
                file_id=1,
                path=active_1,
                row_count=1,
            ),
            make_file(
                file_id=2,
                path=active_2,
                row_count=1,
            ),
            make_file(
                file_id=3,
                path=local_file,
                row_count=1,
                storage_status="local",
            ),
            make_file(
                file_id=4,
                path=superseded,
                row_count=1,
                lifecycle_status="superseded",
            ),
        ]
    )

    assert len(
        plan.groups
    ) == 1

    assert (
        plan.source_file_count
        == 2
    )

    assert (
        plan.source_row_count
        == 2
    )


def test_write_compacted_group_dry_run_does_not_write_output(
    tmp_path,
):

    path_1 = tmp_path / "one.parquet"
    path_2 = tmp_path / "two.parquet"

    rows = [
        {
            "schema_version": 1,
            "record_uid": "same",
            "event_time": datetime(
                2026,
                8,
                26,
                1,
                tzinfo=UTC,
            ),
            "instrument_id": "XKRX:042700",
        }
    ]

    write_parquet(
        path_1,
        rows,
    )

    write_parquet(
        path_2,
        rows,
    )

    plan = build_compaction_plan(
        [
            make_file(
                file_id=1,
                path=path_1,
                row_count=1,
            ),
            make_file(
                file_id=2,
                path=path_2,
                row_count=1,
            ),
        ]
    )

    output_root = tmp_path / "out"

    result = write_compacted_group(
        plan.groups[0],
        output_root=output_root,
        dry_run=True,
    )

    assert result.output is None
    assert result.source_rows == 2
    assert result.replacement_rows == 1
    assert result.duplicate_rows_removed == 1
    assert not output_root.exists()


def test_write_compacted_group_writes_replacement_parquet(
    tmp_path,
):

    path_1 = tmp_path / "one.parquet"
    path_2 = tmp_path / "two.parquet"

    write_parquet(
        path_1,
        [
            {
                "schema_version": 1,
                "record_uid": "uid-a",
                "event_time": datetime(
                    2026,
                    8,
                    26,
                    1,
                    tzinfo=UTC,
                ),
                "instrument_id": "XKRX:042700",
            }
        ],
    )

    write_parquet(
        path_2,
        [
            {
                "schema_version": 1,
                "record_uid": "uid-a",
                "event_time": datetime(
                    2026,
                    8,
                    26,
                    2,
                    tzinfo=UTC,
                ),
                "instrument_id": "XKRX:042700",
            },
            {
                "schema_version": 1,
                "record_uid": "uid-b",
                "event_time": datetime(
                    2026,
                    8,
                    26,
                    3,
                    tzinfo=UTC,
                ),
                "instrument_id": "XKRX:042700",
            },
        ],
    )

    plan = build_compaction_plan(
        [
            make_file(
                file_id=1,
                path=path_1,
                row_count=1,
            ),
            make_file(
                file_id=2,
                path=path_2,
                row_count=2,
            ),
        ]
    )

    result = write_compacted_group(
        plan.groups[0],
        output_root=(
            tmp_path
            /
            "out"
        ),
    )

    assert result.output is not None
    assert result.replacement_rows == 2
    assert result.duplicate_rows_removed == 1

    table = pq.read_table(
        result.output.file_path
    )

    assert (
        table[
            "record_uid"
        ]
        .to_pylist()
        ==
        [
            "uid-a",
            "uid-b",
        ]
    )

    assert (
        result.output.relative_path.as_posix()
        .startswith(
            "site=naver_finance/country=KR/"
            "dataset=forum_post/year=2026/"
            "month=08/day=26/bucket=20/"
        )
    )


class FakeUploader:

    def __init__(
        self,
    ):

        self.calls = []

    def upload_file(
        self,
        local_path,
        relative_path,
    ):

        self.calls.append(
            (
                local_path,
                relative_path,
            )
        )

        return (
            "/remote/"
            +
            Path(
                relative_path
            ).as_posix()
        )


class FakeBaseUploader:

    def __init__(
        self,
    ):

        self.calls = []

    def upload(
        self,
        info,
    ):

        self.calls.append(
            info
        )

        return UploadResult(
            local_path=(
                info.file_path
            ),
            remote_path=(
                "/remote/"
                +
                info.relative_path.as_posix()
            ),
            status="verified",
            local_size=(
                info.file_size
            ),
            remote_size=(
                info.file_size
            ),
        )


class FakeCatalog:

    def __init__(
        self,
    ):

        self.calls = []

    def register_data_file(
        self,
        record,
    ):

        self.calls.append(
            (
                "register",
                record.file_path,
                record.storage_status,
                record.remote_path,
            )
        )

    def mark_uploaded(
        self,
        *,
        file_path,
        remote_path,
    ):

        self.calls.append(
            (
                "mark_uploaded",
                Path(
                    file_path
                ).as_posix(),
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
                Path(
                    file_path
                ).as_posix(),
            )
        )


class FakeCompactionCatalog(
    FakeCatalog
):

    def __init__(
        self,
        files,
    ):

        super().__init__()
        self.files = files

    def list_active_data_files(
        self,
        *,
        site_id,
        dataset,
        country=None,
        partition_date=None,
        bucket=None,
    ):

        del site_id, dataset, country, partition_date, bucket
        return list(
            self.files
        )


def test_publish_compaction_result_uploads_before_superseding_sources(
    tmp_path,
):

    path_1 = tmp_path / "one.parquet"
    path_2 = tmp_path / "two.parquet"

    for path in (
        path_1,
        path_2,
    ):

        write_parquet(
            path,
            [
                {
                    "schema_version": 1,
                    "record_uid": path.stem,
                    "event_time": datetime(
                        2026,
                        8,
                        26,
                        1,
                        tzinfo=UTC,
                    ),
                    "instrument_id": "XKRX:042700",
                }
            ],
        )

    plan = build_compaction_plan(
        [
            make_file(
                file_id=1,
                path=path_1,
                row_count=1,
            ),
            make_file(
                file_id=2,
                path=path_2,
                row_count=1,
            ),
        ]
    )

    result = write_compacted_group(
        plan.groups[0],
        output_root=(
            tmp_path
            /
            "out"
        ),
    )

    catalog = FakeCatalog()
    uploader = FakeUploader()

    remote_path = publish_compaction_result(
        result,
        catalog=catalog,
        uploader=uploader,
    )

    assert remote_path is not None
    assert len(
        uploader.calls
    ) == 1

    call_names = [
        call[0]
        for call in catalog.calls
    ]

    assert call_names == [
        "register",
        "mark_uploaded",
        "mark_superseded",
        "mark_superseded",
    ]


def test_publish_compaction_result_accepts_base_uploader(
    tmp_path,
):

    path_1 = tmp_path / "one.parquet"
    path_2 = tmp_path / "two.parquet"

    for path in (
        path_1,
        path_2,
    ):

        write_parquet(
            path,
            [
                {
                    "schema_version": 1,
                    "record_uid": path.stem,
                    "event_time": datetime(
                        2026,
                        8,
                        26,
                        1,
                        tzinfo=UTC,
                    ),
                    "instrument_id": "XKRX:042700",
                }
            ],
        )

    plan = build_compaction_plan(
        [
            make_file(
                file_id=1,
                path=path_1,
                row_count=1,
            ),
            make_file(
                file_id=2,
                path=path_2,
                row_count=1,
            ),
        ]
    )

    result = write_compacted_group(
        plan.groups[0],
        output_root=(
            tmp_path
            /
            "out"
        ),
    )

    catalog = FakeCatalog()
    uploader = FakeBaseUploader()

    remote_path = publish_compaction_result(
        result,
        catalog=catalog,
        uploader=uploader,
    )

    assert remote_path.startswith(
        "/remote/site=naver_finance/"
    )

    assert len(
        uploader.calls
    ) == 1


def test_compact_active_dataset_replaces_many_active_files(
    tmp_path,
):

    warehouse = tmp_path / "warehouse"
    local_dir = (
        warehouse
        /
        "site=naver_finance"
        /
        "country=KR"
        /
        "dataset=forum_post"
        /
        "year=2026"
        /
        "month=08"
        /
        "day=26"
        /
        "bucket=20"
    )
    local_dir.mkdir(
        parents=True
    )

    path_1 = local_dir / "one.parquet"
    path_2 = local_dir / "two.parquet"

    for path in (
        path_1,
        path_2,
    ):

        write_parquet(
            path,
            [
                {
                    "schema_version": 1,
                    "record_uid": path.stem,
                    "event_time": datetime(
                        2026,
                        8,
                        26,
                        1,
                        tzinfo=UTC,
                    ),
                    "instrument_id": "XKRX:042700",
                }
            ],
        )

    files = [
        CatalogDataFile(
            id=1,
            site_id="naver_finance",
            country="KR",
            dataset="forum_post",
            partition_date="2026-08-26",
            bucket="20",
            file_path=(
                path_1
                .relative_to(
                    warehouse
                )
                .as_posix()
            ),
            sha256="sha",
            row_count=1,
            file_size=path_1.stat().st_size,
            min_event_time=None,
            max_event_time=None,
            schema_version=1,
            storage_status="uploaded",
            lifecycle_status="active",
            remote_path="/remote/one.parquet",
        ),
        CatalogDataFile(
            id=2,
            site_id="naver_finance",
            country="KR",
            dataset="forum_post",
            partition_date="2026-08-26",
            bucket="20",
            file_path=(
                path_2
                .relative_to(
                    warehouse
                )
                .as_posix()
            ),
            sha256="sha",
            row_count=1,
            file_size=path_2.stat().st_size,
            min_event_time=None,
            max_event_time=None,
            schema_version=1,
            storage_status="uploaded",
            lifecycle_status="active",
            remote_path="/remote/two.parquet",
        ),
    ]

    catalog = FakeCompactionCatalog(
        files
    )
    uploader = FakeBaseUploader()

    summary = compact_active_dataset(
        catalog=catalog,
        uploader=uploader,
        site_id="naver_finance",
        country="KR",
        dataset="forum_post",
        warehouse_root=warehouse,
    )

    assert summary.source_files == 2
    assert summary.replacement_files == 1

    call_names = [
        call[0]
        for call in catalog.calls
    ]

    assert call_names == [
        "register",
        "mark_uploaded",
        "mark_superseded",
        "mark_superseded",
    ]
