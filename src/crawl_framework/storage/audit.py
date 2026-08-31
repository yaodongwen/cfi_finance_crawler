from __future__ import annotations

from dataclasses import (
    asdict,
    dataclass,
    field,
)
from pathlib import Path
from typing import Iterable

from crawl_framework.storage.parquet_writer import (
    file_sha256,
)
from crawl_framework.storage.postgres import (
    CatalogDataFile,
)


@dataclass(
    frozen=True,
    slots=True,
)
class StorageAuditIssue:
    code: str

    file_path: str

    message: str

    severity: str = "error"


@dataclass(
    slots=True,
)
class StorageAuditReport:
    files_checked: int = 0

    catalog_errors: int = 0

    missing_remote: int = 0

    size_mismatch: int = 0

    checksum_mismatch: int = 0

    orphans: int = 0

    lifecycle_errors: int = 0

    issues: list[StorageAuditIssue] = field(
        default_factory=list
    )

    def add(
        self,
        issue: StorageAuditIssue,
    ) -> None:

        self.issues.append(
            issue
        )

        if issue.code in {
            "missing_remote_path",
            "duplicate_conflicting_metadata",
        }:

            self.catalog_errors += 1

        elif issue.code == "remote_file_missing":

            self.missing_remote += 1

        elif issue.code == "file_size_mismatch":

            self.size_mismatch += 1

        elif issue.code == "checksum_mismatch":

            self.checksum_mismatch += 1

        elif issue.code == "orphan_remote_file":

            self.orphans += 1

        elif issue.code == "lifecycle_inconsistent":

            self.lifecycle_errors += 1

    def to_dict(
        self,
    ) -> dict:

        return asdict(
            self
        )


def _resolve_remote_path(
    item: CatalogDataFile,
    *,
    remote_root: str | Path | None,
) -> Path | None:

    if not item.remote_path:

        return None

    path = Path(
        item.remote_path
    )

    if path.is_absolute():

        return path

    if remote_root is None:

        return path

    return (
        Path(
            remote_root
        )
        /
        path
    )


def audit_catalog_files(
    files: Iterable[CatalogDataFile],
    *,
    remote_root: str | Path | None = None,
    verify_checksum: bool = True,
) -> StorageAuditReport:
    """
    Read-only audit for Catalog-referenced files.
    """

    report = StorageAuditReport()

    seen_metadata: dict[
        str,
        tuple[str, int, int],
    ] = {}

    for item in files:

        report.files_checked += 1

        metadata = (
            item.sha256,
            int(
                item.row_count
            ),
            int(
                item.file_size
            ),
        )

        previous = seen_metadata.get(
            item.file_path
        )

        if (
            previous is not None
            and
            previous != metadata
        ):

            report.add(
                StorageAuditIssue(
                    code="duplicate_conflicting_metadata",
                    file_path=item.file_path,
                    message=(
                        "duplicate Catalog file_path has "
                        "conflicting metadata"
                    ),
                )
            )

        seen_metadata[
            item.file_path
        ] = metadata

        if (
            item.lifecycle_status == "active"
            and
            item.storage_status != "uploaded"
        ):

            report.add(
                StorageAuditIssue(
                    code="lifecycle_inconsistent",
                    file_path=item.file_path,
                    message=(
                        "active file is not uploaded"
                    ),
                )
            )

        if (
            item.lifecycle_status == "active"
            and
            item.storage_status == "uploaded"
            and
            not item.remote_path
        ):

            report.add(
                StorageAuditIssue(
                    code="missing_remote_path",
                    file_path=item.file_path,
                    message=(
                        "active uploaded file has empty remote_path"
                    ),
                )
            )

            continue

        path = _resolve_remote_path(
            item,
            remote_root=remote_root,
        )

        if (
            item.storage_status == "uploaded"
            and
            path is not None
        ):

            if not path.exists():

                report.add(
                    StorageAuditIssue(
                        code="remote_file_missing",
                        file_path=item.file_path,
                        message=(
                            f"remote file does not exist: {path}"
                        ),
                    )
                )

                continue

            actual_size = (
                path.stat()
                .st_size
            )

            if actual_size != int(
                item.file_size
            ):

                report.add(
                    StorageAuditIssue(
                        code="file_size_mismatch",
                        file_path=item.file_path,
                        message=(
                            "remote file size differs from Catalog"
                        ),
                    )
                )

            if verify_checksum:

                actual_sha256 = file_sha256(
                    path
                )

                if actual_sha256 != item.sha256:

                    report.add(
                        StorageAuditIssue(
                            code="checksum_mismatch",
                            file_path=item.file_path,
                            message=(
                                "remote file checksum differs from Catalog"
                            ),
                        )
                    )

    return report


def audit_orphan_remote_files(
    *,
    remote_root: str | Path,
    catalog_file_paths: Iterable[str],
    pattern: str = "*.parquet",
) -> StorageAuditReport:
    """
    Detect remote Parquet files that have no Catalog row.
    """

    root = Path(
        remote_root
    )

    known = {
        Path(
            path
        ).as_posix()
        for path in catalog_file_paths
    }

    report = StorageAuditReport()

    for path in root.rglob(
        pattern
    ):

        relative = (
            path
            .relative_to(
                root
            )
            .as_posix()
        )

        if relative in known:

            continue

        report.add(
            StorageAuditIssue(
                code="orphan_remote_file",
                file_path=relative,
                message=(
                    "remote file has no Catalog row"
                ),
                severity="warning",
            )
        )

    return report
