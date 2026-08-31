from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol


RepairActionKind = Literal[
    "mark_uploaded",
    "mark_superseded",
    "mark_archived",
]


@dataclass(
    frozen=True,
    slots=True,
)
class StorageRepairAction:
    kind: RepairActionKind

    file_path: str

    remote_path: str | None = None

    reason: str = ""


@dataclass(
    frozen=True,
    slots=True,
)
class StorageRepairResult:
    dry_run: bool

    planned: int

    applied: int

    actions: tuple[StorageRepairAction, ...]


class RepairCatalog(Protocol):

    def mark_uploaded(
        self,
        *,
        file_path: str | Path,
        remote_path: str,
    ) -> None:
        ...

    def mark_superseded(
        self,
        *,
        file_path: str | Path,
    ) -> None:
        ...

    def mark_archived(
        self,
        *,
        file_path: str | Path,
    ) -> None:
        ...


def validate_repair_action(
    action: StorageRepairAction,
) -> None:

    if not str(
        action.file_path
    ).strip():

        raise ValueError(
            "repair action file_path cannot be empty"
        )

    if (
        action.kind == "mark_uploaded"
        and
        not str(
            action.remote_path
            or ""
        ).strip()
    ):

        raise ValueError(
            "mark_uploaded repair requires remote_path"
        )


def apply_safe_repairs(
    actions,
    *,
    catalog: RepairCatalog,
    dry_run: bool = True,
) -> StorageRepairResult:
    """
    Apply explicitly requested safe Catalog repairs.

    No deletion is performed here. Automatic repair planning from audit
    findings is intentionally left to higher-level policy.
    """

    normalized = tuple(
        actions
    )

    for action in normalized:

        validate_repair_action(
            action
        )

    if dry_run:

        return StorageRepairResult(
            dry_run=True,
            planned=len(
                normalized
            ),
            applied=0,
            actions=normalized,
        )

    applied = 0

    for action in normalized:

        if action.kind == "mark_uploaded":

            catalog.mark_uploaded(
                file_path=action.file_path,
                remote_path=str(
                    action.remote_path
                ),
            )

        elif action.kind == "mark_superseded":

            catalog.mark_superseded(
                file_path=action.file_path,
            )

        elif action.kind == "mark_archived":

            catalog.mark_archived(
                file_path=action.file_path,
            )

        else:

            raise ValueError(
                f"unsupported repair action: {action.kind!r}"
            )

        applied += 1

    return StorageRepairResult(
        dry_run=False,
        planned=len(
            normalized
        ),
        applied=applied,
        actions=normalized,
    )
