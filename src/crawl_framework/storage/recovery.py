from __future__ import annotations

import hashlib
import json
import os

from dataclasses import (
    asdict,
    dataclass,
    replace,
)
from datetime import (
    date,
    datetime,
    timezone,
)
from pathlib import Path
from typing import (
    Literal,
    Protocol,
)

from crawl_framework.storage.record_index import (
    RecordIndexReader,
)
from crawl_framework.storage.cleaner import (
    Cleaner,
    CleanupContext,
)
from crawl_framework.storage.parquet_writer import (
    ParquetFileInfo,
)
from crawl_framework.storage.partition import (
    PartitionKey,
)
from crawl_framework.storage.postgres import (
    PostgresCatalog,
)
from crawl_framework.storage.seen_store import (
    SeenStore,
)
from crawl_framework.storage.uploader import (
    BaseUploader,
)
from crawl_framework.storage.retry_policy import (
    RetryPolicy,
    RetryPolicyResult,
)


# ============================================================
# Recovery stage
# ============================================================


RecoveryStage = Literal[
    "local",
    "uploaded",
    "verified",
    "catalog_registered",
    "seen_committed",
    "checkpoint_committed",
    "cleanable",
    "deleted",
    "failed",
]


STAGE_ORDER: dict[
    str,
    int,
] = {
    "local": 10,
    "uploaded": 20,
    "verified": 30,
    "catalog_registered": 40,
    "seen_committed": 50,
    "checkpoint_committed": 60,
    "cleanable": 70,
    "deleted": 80,
    "failed": -1,
}


# ============================================================
# Helpers
# ============================================================


def utc_now_iso() -> str:

    return (
        datetime.now(
            timezone.utc
        )
        .isoformat(
            timespec="seconds"
        )
        .replace(
            "+00:00",
            "Z",
        )
    )


def safe_manifest_name(
    manifest_id: str,
) -> str:

    value = str(
        manifest_id
    ).strip()

    if not value:

        raise ValueError(
            "manifest_id cannot be empty"
        )

    return (
        hashlib.sha256(
            value.encode(
                "utf-8"
            )
        )
        .hexdigest()
        + ".json"
    )


# ============================================================
# Manifest
# ============================================================


@dataclass(
    frozen=True,
    slots=True,
)
class RecoveryManifest:
    """
    一个 Parquet 文件的 durable lifecycle。

    注意：

    manifest 本身不是数据真相。

    它只是 crash recovery 的本地状态记录。
    """

    manifest_id: str

    local_path: str

    relative_path: str

    site_id: str

    country: str

    dataset: str

    sha256: str

    row_count: int

    file_size: int

    scope_tokens: tuple[str, ...] = ()

    stage: RecoveryStage = "local"

    record_index_path: str | None = None

    remote_path: str | None = None

    last_error: str | None = None

    failed_from_stage: RecoveryStage | None = None

    retry_count: int = 0

    last_failed_at: str | None = None

    retryable: bool | None = None

    retry_reason: str | None = None

    created_at: str = ""

    updated_at: str = ""


    def __post_init__(
        self,
    ) -> None:

        now = utc_now_iso()

        if not self.created_at:

            object.__setattr__(
                self,
                "created_at",
                now,
            )

        if not self.updated_at:

            object.__setattr__(
                self,
                "updated_at",
                now,
            )

        if not self.manifest_id.strip():

            raise ValueError(
                "manifest_id cannot be empty"
            )

        if not self.relative_path.strip():

            raise ValueError(
                "relative_path cannot be empty"
            )

        if self.stage not in STAGE_ORDER:

            raise ValueError(
                f"invalid stage: {self.stage!r}"
            )

        if self.retry_count < 0:

            raise ValueError(
                "retry_count cannot be negative"
            )

        normalized_scope_tokens = tuple(
            sorted(
                {
                    str(token).strip()
                    for token in self.scope_tokens
                    if str(token).strip()
                }
            )
        )
        object.__setattr__(
            self,
            "scope_tokens",
            normalized_scope_tokens,
        )

        if (
            self.failed_from_stage
            is not None
            and self.failed_from_stage
            not in STAGE_ORDER
        ):

            raise ValueError(
                "invalid failed_from_stage: "
                f"{self.failed_from_stage!r}"
            )

        if (
            self.failed_from_stage
            == "failed"
        ):

            raise ValueError(
                "failed_from_stage cannot be 'failed'"
            )


    def advance(
        self,
        stage: RecoveryStage,
        *,
        remote_path: str | None = None,
        last_error: str | None = None,
    ) -> "RecoveryManifest":
        """
        正常生命周期阶段推进。

        规则：

        1. 普通阶段只能向前推进，不能倒退。
        2. 可以从任意正常阶段进入 failed。
        3. failed -> 正常阶段的恢复不走这里，
           而由 RecoveryManager._force_stage() 负责。
        4. retry_count 保留历史累计值。
        """

        if stage not in STAGE_ORDER:

            raise ValueError(
                f"invalid stage: {stage!r}"
            )

        # ====================================================
        # 禁止正常生命周期倒退
        #
        # 例如：
        #
        # verified -> uploaded
        #
        # 是非法的。
        #
        # failed 的恢复由 _force_stage() 单独处理，
        # 所以这里不允许借 advance() 做恢复。
        # ====================================================

        if (
            stage != "failed"
            and self.stage != "failed"
            and STAGE_ORDER[
                stage
            ]
            < STAGE_ORDER[
                self.stage
            ]
        ):

            raise ValueError(
                "cannot move recovery stage "
                f"backward: "
                f"{self.stage} -> {stage}"
            )

        # ====================================================
        # failed -> 正常阶段
        #
        # 不允许通过普通 advance() 恢复。
        # 必须由 RecoveryManager._force_stage() 显式执行。
        # ====================================================

        if (
            self.stage == "failed"
            and stage != "failed"
        ):

            raise ValueError(
                "cannot recover failed manifest "
                "using advance(); "
                "use RecoveryManager._force_stage()"
            )

        # ====================================================
        # 正常阶段推进
        # ====================================================

        if stage != "failed":

            return replace(
                self,
                stage=stage,
                remote_path=(
                    remote_path
                    if remote_path is not None
                    else self.remote_path
                ),
                last_error=last_error,
                failed_from_stage=None,
                last_failed_at=None,
                retryable=None,
                retry_reason=None,
                updated_at=utc_now_iso(),
            )

        # ====================================================
        # 进入 failed
        #
        # retry_count / failed_from_stage
        # 应由 RecoveryStore.mark_failed() 管理。
        #
        # advance("failed") 只保留兼容能力。
        # ====================================================

        return replace(
            self,
            stage="failed",
            remote_path=(
                remote_path
                if remote_path is not None
                else self.remote_path
            ),
            last_error=last_error,
            updated_at=utc_now_iso(),
        )

# ============================================================
# Store
# ============================================================


class RecoveryStore:
    """
    Crash-safe JSON manifest store。
    """

    def __init__(
        self,
        root: str | Path,
    ) -> None:

        self.root = Path(
            root
        )

        self.root.mkdir(
            parents=True,
            exist_ok=True,
        )


    def path_for(
        self,
        manifest_id: str,
    ) -> Path:

        return (
            self.root
            / safe_manifest_name(
                manifest_id
            )
        )


    def save(
        self,
        manifest: RecoveryManifest,
    ) -> None:

        path = self.path_for(
            manifest.manifest_id
        )

        payload = asdict(
            manifest
        )

        tmp = path.with_name(
            path.name
            + ".tmp"
        )

        try:

            with tmp.open(
                "w",
                encoding="utf-8",
            ) as file:

                json.dump(
                    payload,
                    file,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )

                file.write(
                    "\n"
                )

                file.flush()

                os.fsync(
                    file.fileno()
                )

            os.replace(
                tmp,
                path,
            )

        finally:

            if tmp.exists():

                try:
                    tmp.unlink()

                except OSError:
                    pass


    def load(
        self,
        manifest_id: str,
    ) -> RecoveryManifest | None:

        path = self.path_for(
            manifest_id
        )

        if not path.exists():

            return None

        try:

            obj = json.loads(
                path.read_text(
                    encoding="utf-8"
                )
            )

        except Exception as exc:

            raise RuntimeError(
                "recovery manifest read failed: "
                f"{path}"
            ) from exc

        return RecoveryManifest(
            **obj
        )


    def delete(
        self,
        manifest_id: str,
    ) -> bool:

        path = self.path_for(
            manifest_id
        )

        if not path.exists():

            return False

        path.unlink()

        return True


    def list_all(
        self,
    ) -> list[
        RecoveryManifest
    ]:

        manifests: list[
            RecoveryManifest
        ] = []

        for path in sorted(
            self.root.glob(
                "*.json"
            )
        ):

            try:

                obj = json.loads(
                    path.read_text(
                        encoding="utf-8"
                    )
                )

                manifests.append(
                    RecoveryManifest(
                        **obj
                    )
                )

            except Exception as exc:

                raise RuntimeError(
                    "invalid recovery manifest: "
                    f"{path}"
                ) from exc

        return manifests


    def list_pending(
        self,
    ) -> list[
        RecoveryManifest
    ]:

        return [
            manifest
            for manifest
            in self.list_all()
            if manifest.stage
            != "deleted"
        ]


    def list_by_stage(
        self,
        stage: RecoveryStage,
    ) -> list[
        RecoveryManifest
    ]:

        return [
            manifest
            for manifest
            in self.list_all()
            if manifest.stage
            == stage
        ]

    def list_for_scope(
        self,
        scope_token: str,
        *,
        include_deleted: bool = False,
    ) -> list[RecoveryManifest]:
        token = str(scope_token).strip()
        if not token:
            raise ValueError("scope_token cannot be empty")
        return [
            manifest
            for manifest in self.list_all()
            if token in manifest.scope_tokens
            and (include_deleted or manifest.stage != "deleted")
        ]


    def advance(
        self,
        manifest_id: str,
        stage: RecoveryStage,
        *,
        remote_path: str | None = None,
        last_error: str | None = None,
    ) -> RecoveryManifest:

        current = self.load(
            manifest_id
        )

        if current is None:

            raise KeyError(
                "recovery manifest "
                f"not found: {manifest_id!r}"
            )

        updated = current.advance(
            stage,
            remote_path=remote_path,
            last_error=last_error,
        )

        self.save(
            updated
        )

        return updated


    def mark_failed(
        self,
        manifest_id: str,
        error: str,
        *,
        failed_from_stage: RecoveryStage | None = None,
        retryable: bool | None = None,
        retry_reason: str | None = None,
    ) -> RecoveryManifest:
        """
        标记一次 recovery failure。

        failed_from_stage:
            失败发生前最后一个 durable stage。

        retry_count:
            每失败一次 +1。

        retryable:
            True:
                可以继续自动恢复。

            False:
                terminal failure，不自动恢复。

            None:
                尚未经过 RetryPolicy 判断。
        """

        current = self.load(
            manifest_id
        )

        if current is None:

            raise KeyError(
                "recovery manifest "
                f"not found: {manifest_id!r}"
            )

        if (
            failed_from_stage
            is None
        ):

            if current.stage == "failed":

                failed_from_stage = (
                    current.failed_from_stage
                )

            else:

                failed_from_stage = (
                    current.stage
                )

        if (
            failed_from_stage
            == "failed"
        ):

            raise ValueError(
                "failed_from_stage cannot be 'failed'"
            )

        now = utc_now_iso()

        updated = replace(
            current,
            stage="failed",
            last_error=str(
                error
            ),
            failed_from_stage=(
                failed_from_stage
            ),
            retry_count=(
                current.retry_count
                + 1
            ),
            last_failed_at=now,
            retryable=retryable,
            retry_reason=retry_reason,
            updated_at=now,
        )

        self.save(
            updated
        )

        return updated


    def reclassify_failed(
        self,
        manifest_id: str,
        *,
        retryable: bool,
        retry_reason: str,
    ) -> RecoveryManifest:
        """
        更新 failed manifest 的 retry classification。

        这是非破坏性 repair：不删除 manifest，
        不改变 failed_from_stage，不增加 retry_count，
        只允许此前误判的 terminal failure 再次进入
        startup recovery。
        """

        current = self.load(
            manifest_id
        )

        if current is None:

            raise KeyError(
                "recovery manifest "
                f"not found: {manifest_id!r}"
            )

        if current.stage != "failed":

            raise ValueError(
                "only failed manifests can be "
                "reclassified"
            )

        updated = replace(
            current,
            retryable=bool(
                retryable
            ),
            retry_reason=str(
                retry_reason
            ),
            updated_at=utc_now_iso(),
        )

        self.save(
            updated
        )

        return updated

# ============================================================
# Recovery result
# ============================================================


RecoveryAction = Literal[
    "skipped",
    "uploaded",
    "registered",
    "seen_committed",
    "cleaned",
    "failed",
]


@dataclass(
    frozen=True,
    slots=True,
)
class RecoveryResult:
    """
    单个 manifest 恢复结果。
    """

    manifest_id: str

    original_stage: RecoveryStage

    final_stage: RecoveryStage

    action: RecoveryAction

    success: bool

    message: str | None = None


@dataclass(
    slots=True,
)
class RecoveryStats:

    scanned: int = 0

    recovered: int = 0

    skipped: int = 0

    failed: int = 0


class RecoveryFaultInjector(Protocol):

    def after_stage(
        self,
        manifest: RecoveryManifest,
        stage: RecoveryStage,
    ) -> None:
        ...


class NoopRecoveryFaultInjector:

    def after_stage(
        self,
        manifest: RecoveryManifest,
        stage: RecoveryStage,
    ) -> None:

        return None

    deleted: int = 0


# ============================================================
# Manifest -> ParquetFileInfo
# ============================================================


def manifest_to_parquet_info(
    manifest: RecoveryManifest,
) -> ParquetFileInfo:
    """
    从 RecoveryManifest 重建 ParquetFileInfo。

    stocklake/v2 标准路径：

        site=...
        country=...
        dataset=...
        year=YYYY
        month=MM
        day=DD
        bucket=xx
        part-....parquet

    注意：

    PartitionKey.partition_date 必须保持 date 类型，
    不能使用字符串。

    因为 PostgreSQL catalog 层后续会调用：

        partition_date.isoformat()
    """

    parts = Path(
        manifest.relative_path
    ).parts

    values: dict[
        str,
        str,
    ] = {}

    for part in parts:

        if "=" not in part:
            continue

        key, value = part.split(
            "=",
            1,
        )

        values[
            key
        ] = value

    required = [
        "site",
        "country",
        "dataset",
        "year",
        "month",
        "day",
        "bucket",
    ]

    missing = [
        key
        for key
        in required
        if key not in values
    ]

    if missing:

        raise ValueError(
            "cannot reconstruct partition "
            f"from manifest path; "
            f"missing={missing!r}, "
            f"path={manifest.relative_path!r}"
        )

    # ========================================================
    # 关键修复：
    #
    # 原来这里生成：
    #
    #     "2026-08-24"
    #
    # 是 str。
    #
    # PartitionKey 实际需要 datetime.date。
    # ========================================================

    from datetime import date

    try:

        partition_date = date(
            int(
                values[
                    "year"
                ]
            ),
            int(
                values[
                    "month"
                ]
            ),
            int(
                values[
                    "day"
                ]
            ),
        )

    except (
        TypeError,
        ValueError,
    ) as exc:

        raise ValueError(
            "invalid partition date in "
            "recovery manifest path: "
            f"year={values.get('year')!r}, "
            f"month={values.get('month')!r}, "
            f"day={values.get('day')!r}"
        ) from exc

    # ========================================================
    # Bucket
    #
    # 当前路径 bucket 使用十六进制：
    #
    #     bucket=00
    #     bucket=5f
    #     bucket=ff
    #
    # PartitionKey 内部保存整数。
    # ========================================================

    try:

        bucket = int(
            values[
                "bucket"
            ],
            16,
        )

    except ValueError as exc:

        raise ValueError(
            "invalid recovery bucket: "
            f"{values['bucket']!r}"
        ) from exc

    partition = PartitionKey(
        site_id=values[
            "site"
        ],
        country=values[
            "country"
        ],
        dataset=values[
            "dataset"
        ],
        partition_date=partition_date,
        bucket=bucket,
    )

    return ParquetFileInfo(
        file_path=Path(
            manifest.local_path
        ),
        relative_path=Path(
            manifest.relative_path
        ),
        partition=partition,
        row_count=(
            manifest.row_count
        ),
        file_size=(
            manifest.file_size
        ),
        sha256=(
            manifest.sha256
        ),
        min_event_time=None,
        max_event_time=None,
        schema_version=1,
    )

# ============================================================
# Recovery Manager
# ============================================================


class RecoveryManager:
    """
    真正执行 crash recovery。

    当前恢复原则：

    local / failed:
        本地文件还在
            → upload
            → verify
            → catalog

    uploaded:
        使用 manifest 的 remote_path + size/hash durable evidence
        仅验证已存在的远端文件，不重复上传。

    verified:
        → register PostgreSQL

    catalog_registered:
        无法仅凭 manifest 重建每条 record_uid，
        所以不伪造 SeenStore commit。
        保留 manifest，等待 Runtime 重抓后由 SeenStore 正常提交。

    seen_committed:
        Runtime checkpoint 采用 at-least-once，
        不依赖 manifest 恢复 checkpoint。
        因此可推进 cleanable。

    checkpoint_committed:
        → cleanable

    cleanable:
        → Cleaner

    deleted:
        skip
    """

    def __init__(
        self,
        *,
        store: RecoveryStore,
        uploader: BaseUploader,
        catalog: PostgresCatalog,
        cleaner: Cleaner,
        seen_store: SeenStore | None = None,
        record_index_reader: RecordIndexReader | None = None,
        retry_policy: RetryPolicy | None = None,
        fault_injector: RecoveryFaultInjector | None = None,
    ) -> None:
        """
        RecoveryManager 初始化。

        retry_policy:
            控制 failed manifest 是否继续自动重试。

            如果未显式传入，
            默认使用 RetryPolicy()。
        """

        self.store = store

        self.uploader = uploader

        self.catalog = catalog

        self.cleaner = cleaner

        self.seen_store = seen_store

        self.record_index_reader = (
            record_index_reader
            or RecordIndexReader()
        )

        self.retry_policy = (
            retry_policy
            or RetryPolicy()
        )

        self.fault_injector = (
            fault_injector
            or NoopRecoveryFaultInjector()
        )

        self.stats = RecoveryStats()

    def _after_stage(
        self,
        manifest: RecoveryManifest,
        stage: RecoveryStage,
    ) -> None:

        self.fault_injector.after_stage(
            manifest,
            stage,
        )

    def recover_all(
        self,
    ) -> list[
        RecoveryResult
    ]:

        results: list[
            RecoveryResult
        ] = []

        for manifest in (
            self.store.list_all()
        ):

            self.stats.scanned += 1

            result = self.recover_one(
                manifest
            )

            results.append(
                result
            )

            if result.success:

                if result.action == "skipped":

                    self.stats.skipped += 1

                else:

                    self.stats.recovered += 1

                if result.final_stage == "deleted":

                    self.stats.deleted += 1

            else:

                self.stats.failed += 1

        return results


    def recover_pending(
        self,
    ) -> list[
        RecoveryResult
    ]:
        """
        只扫描非 deleted manifest。
        """

        results: list[
            RecoveryResult
        ] = []

        for manifest in (
            self.store.list_pending()
        ):

            self.stats.scanned += 1

            result = self.recover_one(
                manifest
            )

            results.append(
                result
            )

            if result.success:

                if result.action == "skipped":

                    self.stats.skipped += 1

                else:

                    self.stats.recovered += 1

                if result.final_stage == "deleted":

                    self.stats.deleted += 1

            else:

                self.stats.failed += 1

        return results


    def recover_one(
        self,
        manifest: RecoveryManifest,
    ) -> RecoveryResult:
        """
        恢复单个 RecoveryManifest。

        生命周期：

        deleted
            -> skip

        failed
            -> RetryPolicy
            -> terminal: skip
            -> retryable:
               从 failed_from_stage 继续

        local
            -> upload
            -> verify

        uploaded
            -> upload / verify

        verified
            -> PostgreSQL catalog

        catalog_registered
            -> 如果有 sidecar + SeenStore：
               SeenStore.commit_many()

            -> 如果没有 sidecar：
               保留 catalog_registered

        seen_committed
            -> checkpoint_committed

        checkpoint_committed
            -> cleanable

        cleanable
            -> Cleaner
            -> deleted

        任意异常：
            -> RetryPolicy
            -> mark_failed()
        """

        original_stage = (
            manifest.stage
        )

        # 记录本次 recover_one 是否实际完成了
        # PostgreSQL catalog registration。
        #
        # 用于区分：
        #
        # verified -> catalog_registered -> 无 sidecar
        #
        # 这种情况虽然不能继续 SeenStore，
        # 但本轮实际上已经完成了有效恢复工作，
        # 不能统计成 skipped。
        catalog_registered_this_run = False

        # ====================================================
        # 0. Failed manifest decision
        # ====================================================

        if manifest.stage == "failed":

            # ------------------------------------------------
            # 已明确 terminal failure。
            # ------------------------------------------------

            if manifest.retryable is False:

                return RecoveryResult(
                    manifest_id=(
                        manifest.manifest_id
                    ),
                    original_stage=(
                        original_stage
                    ),
                    final_stage="failed",
                    action="skipped",
                    success=True,
                    message=(
                        "terminal recovery failure: "
                        f"{manifest.retry_reason or 'unknown'}"
                    ),
                )

            # ------------------------------------------------
            # 兼容旧 manifest：
            #
            # stage=failed，
            # 但 retryable 尚未分类。
            # ------------------------------------------------

            if (
                manifest.retryable is None
                and manifest.last_error
            ):

                policy_result = (
                    self.retry_policy.classify(
                        manifest.last_error,
                        retry_count=(
                            manifest.retry_count
                        ),
                    )
                )

                manifest = replace(
                    manifest,
                    retryable=(
                        policy_result.should_retry
                    ),
                    retry_reason=(
                        policy_result.reason
                    ),
                    updated_at=utc_now_iso(),
                )

                self.store.save(
                    manifest
                )

                if not (
                    policy_result.should_retry
                ):

                    return RecoveryResult(
                        manifest_id=(
                            manifest.manifest_id
                        ),
                        original_stage=(
                            original_stage
                        ),
                        final_stage="failed",
                        action="skipped",
                        success=True,
                        message=(
                            "recovery retry stopped: "
                            f"{policy_result.reason}"
                        ),
                    )

            # ------------------------------------------------
            # failed manifest 没有恢复点：
            # 不能猜。
            # ------------------------------------------------

            if (
                manifest.failed_from_stage
                is None
            ):

                return RecoveryResult(
                    manifest_id=(
                        manifest.manifest_id
                    ),
                    original_stage=(
                        original_stage
                    ),
                    final_stage="failed",
                    action="skipped",
                    success=True,
                    message=(
                        "failed manifest has no "
                        "failed_from_stage"
                    ),
                )

            # ------------------------------------------------
            # 从失败前最后一个 durable stage 恢复。
            # ------------------------------------------------

            manifest = (
                self._force_stage(
                    manifest,
                    manifest.failed_from_stage,
                )
            )

        try:

            # =================================================
            # 1. Already deleted
            # =================================================

            if manifest.stage == "deleted":

                return RecoveryResult(
                    manifest_id=(
                        manifest.manifest_id
                    ),
                    original_stage=(
                        original_stage
                    ),
                    final_stage="deleted",
                    action="skipped",
                    success=True,
                    message=(
                        "manifest already deleted"
                    ),
                )

            # =================================================
            # 2. local
            # =================================================

            if manifest.stage == "local":

                local_path = Path(
                    manifest.local_path
                )

                if not local_path.exists():

                    raise RuntimeError(
                        "cannot recover manifest: "
                        "local parquet is missing"
                    )

                info = (
                    manifest_to_parquet_info(
                        manifest
                    )
                )

                upload = (
                    self.uploader.upload(
                        info
                    )
                )

                if upload.status not in {
                    "uploaded",
                    "verified",
                }:

                    raise RuntimeError(
                        "recovery upload failed: "
                        f"status={upload.status!r}"
                    )

                manifest = (
                    self._force_stage(
                        manifest,
                        "uploaded",
                        remote_path=(
                            upload.remote_path
                        ),
                    )
                )

                self._after_stage(
                    manifest,
                    "uploaded",
                )

                if upload.status != "verified":

                    raise RuntimeError(
                        "recovery upload was not verified"
                    )

                manifest = (
                    self.store.advance(
                        manifest.manifest_id,
                        "verified",
                        remote_path=(
                            upload.remote_path
                        ),
                    )
                )

                self._after_stage(
                    manifest,
                    "verified",
                )

            # =================================================
            # 3. uploaded
            # =================================================

            elif manifest.stage == "uploaded":
                info = (
                    manifest_to_parquet_info(
                        manifest
                    )
                )

                remote_path = str(manifest.remote_path or "").strip()
                if not remote_path:
                    raise RuntimeError(
                        "cannot verify uploaded manifest: remote_path is missing"
                    )

                verification = self.uploader.verify_existing(
                    info,
                    remote_path=remote_path,
                )

                if verification.status != "verified":
                    raise RuntimeError(
                        "recovery verification failed"
                    )

                manifest = (
                    self.store.advance(
                        manifest.manifest_id,
                        "verified",
                        remote_path=(
                            verification.remote_path
                        ),
                    )
                )

                self._after_stage(
                    manifest,
                    "verified",
                )

            # =================================================
            # 4. verified -> PostgreSQL catalog
            # =================================================

            if manifest.stage == "verified":

                info = (
                    manifest_to_parquet_info(
                        manifest
                    )
                )

                # =============================================
                # Register file in PostgreSQL catalog
                # =============================================

                self.catalog.register_parquet_file(
                    info
                )

                # =============================================
                # If remote_path is known, mark uploaded.
                #
                # 新版正常 uploader / recovery manifest
                # 会带 remote_path。
                #
                # 但为了兼容旧 manifest 和现有测试 fixture，
                # remote_path 缺失时暂时不阻断恢复。
                # =============================================

                remote_path = (
                    str(
                        manifest.remote_path
                    ).strip()
                    if manifest.remote_path
                    else ""
                )

                if remote_path:

                    self.catalog.mark_uploaded(
                        file_path=(
                            info.relative_path
                        ),
                        remote_path=(
                            remote_path
                        ),
                    )

                # =============================================
                # Advance recovery state
                # =============================================

                manifest = (
                    self.store.advance(
                        manifest.manifest_id,
                        "catalog_registered",
                    )
                )

                catalog_registered_this_run = True              

                self._after_stage(
                    manifest,
                    "catalog_registered",
                )

            # =================================================
            # 5. catalog_registered -> SeenStore
            # =================================================

            if (
                manifest.stage
                == "catalog_registered"
            ):

                # ------------------------------------------------
                # 没有 SeenStore：
                #
                # 如果本轮刚完成 catalog registration，
                # 这仍然属于一次 successful recovery。
                # ------------------------------------------------

                if self.seen_store is None:

                    return RecoveryResult(
                        manifest_id=(
                            manifest.manifest_id
                        ),
                        original_stage=(
                            original_stage
                        ),
                        final_stage=(
                            manifest.stage
                        ),
                        action=(
                            "registered"
                            if catalog_registered_this_run
                            else "skipped"
                        ),
                        success=True,
                        message=(
                            "postgres catalog registered; "
                            "seen_store is not configured"
                            if catalog_registered_this_run
                            else
                            "catalog is durable but "
                            "seen_store is not configured"
                        ),
                    )

                # ------------------------------------------------
                # 没有 sidecar：
                #
                # 同理：
                #
                # 如果是 verified -> catalog_registered
                # 本轮确实完成了恢复工作。
                # ------------------------------------------------

                if not (
                    manifest.record_index_path
                ):

                    return RecoveryResult(
                        manifest_id=(
                            manifest.manifest_id
                        ),
                        original_stage=(
                            original_stage
                        ),
                        final_stage=(
                            manifest.stage
                        ),
                        action=(
                            "registered"
                            if catalog_registered_this_run
                            else "skipped"
                        ),
                        success=True,
                        message=(
                            "postgres catalog registered; "
                            "record_index_path is missing"
                            if catalog_registered_this_run
                            else
                            "catalog is durable but "
                            "record_index_path is missing"
                        ),
                    )

                index_path = Path(
                    manifest.record_index_path
                )

                if not index_path.exists():

                    raise RuntimeError(
                        "record index sidecar missing: "
                        f"{index_path}"
                    )

                pairs = (
                    self.record_index_reader
                    .read_pairs(
                        index_path
                    )
                )

                if (
                    len(pairs)
                    != manifest.row_count
                ):

                    raise RuntimeError(
                        "record index row count mismatch: "
                        f"manifest={manifest.row_count}, "
                        f"index={len(pairs)}"
                    )

                self.seen_store.commit_many(
                    pairs
                )

                manifest = (
                    self.store.advance(
                        manifest.manifest_id,
                        "seen_committed",
                    )
                )

                self._after_stage(
                    manifest,
                    "seen_committed",
                )

            # =================================================
            # 6. seen_committed
            # =================================================

            if (
                manifest.stage
                == "seen_committed"
            ):

                manifest = (
                    self.store.advance(
                        manifest.manifest_id,
                        "checkpoint_committed",
                    )
                )

                self._after_stage(
                    manifest,
                    "checkpoint_committed",
                )

            # =================================================
            # 7. checkpoint_committed -> cleanable
            # =================================================

            if (
                manifest.stage
                == "checkpoint_committed"
            ):

                manifest = (
                    self.store.advance(
                        manifest.manifest_id,
                        "cleanable",
                    )
                )

                self._after_stage(
                    manifest,
                    "cleanable",
                )

            # =================================================
            # 8. cleanable
            # =================================================

            if manifest.stage == "cleanable":

                info = (
                    manifest_to_parquet_info(
                        manifest
                    )
                )

                cleanup = (
                    self.cleaner.clean(
                        info,
                        CleanupContext(
                            uploaded=True,
                            verified=True,
                            catalog_registered=True,
                            seen_committed=True,
                            checkpoint_committed=True,
                        ),
                    )
                )

                # ---------------------------------------------
                # 正常 Cleaner 删除成功。
                # ---------------------------------------------

                if cleanup.deleted:

                    manifest = (
                        self.store.advance(
                            manifest.manifest_id,
                            "deleted",
                        )
                    )

                    return RecoveryResult(
                        manifest_id=(
                            manifest.manifest_id
                        ),
                        original_stage=(
                            original_stage
                        ),
                        final_stage="deleted",
                        action="cleaned",
                        success=True,
                        message=(
                            "local parquet and recovery "
                            "sidecar cleaned"
                        ),
                    )

                # ---------------------------------------------
                # Crash boundary：
                #
                # 文件可能已经删掉，
                # 但 manifest 还没来得及 advance。
                # ---------------------------------------------

                local_missing = (
                    not Path(
                        manifest.local_path
                    ).exists()
                )

                index_missing = True

                if (
                    manifest.record_index_path
                    is not None
                ):

                    index_missing = (
                        not Path(
                            manifest.record_index_path
                        ).exists()
                    )

                if (
                    local_missing
                    and index_missing
                ):

                    manifest = (
                        self.store.advance(
                            manifest.manifest_id,
                            "deleted",
                        )
                    )

                    return RecoveryResult(
                        manifest_id=(
                            manifest.manifest_id
                        ),
                        original_stage=(
                            original_stage
                        ),
                        final_stage="deleted",
                        action="cleaned",
                        success=True,
                        message=(
                            "local parquet and sidecar "
                            "already absent"
                        ),
                    )

                if (
                    local_missing
                    and not index_missing
                ):

                    raise RuntimeError(
                        "cleaner left recovery sidecar "
                        "after parquet deletion"
                    )

                raise RuntimeError(
                    "cleaner did not delete "
                    "cleanable parquet"
                )

            # =================================================
            # 9. No further action
            # =================================================

            return RecoveryResult(
                manifest_id=(
                    manifest.manifest_id
                ),
                original_stage=(
                    original_stage
                ),
                final_stage=(
                    manifest.stage
                ),
                action=(
                    "registered"
                    if catalog_registered_this_run
                    else "skipped"
                ),
                success=True,
                message=(
                    "postgres catalog registered"
                    if catalog_registered_this_run
                    else
                    "no recovery action required"
                ),
            )

        # ====================================================
        # Recovery failure
        # ====================================================

        except Exception as exc:

            try:

                current = self.store.load(
                    manifest.manifest_id
                )

                if current is not None:

                    if (
                        current.stage
                        == "failed"
                    ):

                        failed_from_stage = (
                            current.failed_from_stage
                        )

                    else:

                        failed_from_stage = (
                            current.stage
                        )

                    # mark_failed() 会 retry_count +1，
                    # 所以 policy 使用失败后的次数。
                    next_retry_count = (
                        current.retry_count
                        + 1
                    )

                    policy_result = (
                        self.retry_policy.classify(
                            exc,
                            retry_count=(
                                next_retry_count
                            ),
                        )
                    )

                    self.store.mark_failed(
                        manifest.manifest_id,
                        str(
                            exc
                        ),
                        failed_from_stage=(
                            failed_from_stage
                        ),
                        retryable=(
                            policy_result.should_retry
                        ),
                        retry_reason=(
                            policy_result.reason
                        ),
                    )

            except Exception:
                pass

            return RecoveryResult(
                manifest_id=(
                    manifest.manifest_id
                ),
                original_stage=(
                    original_stage
                ),
                final_stage="failed",
                action="failed",
                success=False,
                message=str(
                    exc
                ),
            )

    # ========================================================
    # Force stage helper
    # ========================================================

    def _force_stage(
        self,
        manifest: RecoveryManifest,
        stage: RecoveryStage,
        *,
        remote_path: str | None = None,
        last_error: str | None = None,
    ) -> RecoveryManifest:
        """
        Recovery 专用状态修复。

        普通 RecoveryManifest.advance()
        负责正常阶段推进。

        crash recovery 场景下，
        failed manifest 需要恢复到：

            failed -> local
            failed -> uploaded
            failed -> verified
            failed -> catalog_registered
            failed -> seen_committed
            failed -> checkpoint_committed
            failed -> cleanable

        因此这里允许 RecoveryManager
        直接恢复 manifest.stage。

        retry_count 不清零，
        因为它代表该 manifest 历史累计失败次数。

        当恢复到正常阶段时：

            failed_from_stage -> None
            last_failed_at    -> None

        表示当前已经退出 failed 状态。
        """

        if stage not in STAGE_ORDER:

            raise ValueError(
                f"invalid recovery stage: {stage!r}"
            )

        updated = replace(
            manifest,
            stage=stage,
            remote_path=(
                remote_path
                if remote_path is not None
                else manifest.remote_path
            ),
            last_error=last_error,
            failed_from_stage=(
                None
                if stage != "failed"
                else manifest.failed_from_stage
            ),
            last_failed_at=(
                None
                if stage != "failed"
                else manifest.last_failed_at
            ),
            retryable=(
                None
                if stage != "failed"
                else manifest.retryable
            ),
            retry_reason=(
                None
                if stage != "failed"
                else manifest.retry_reason
            ),
            updated_at=utc_now_iso(),
        )

        self.store.save(
            updated
        )

        return updated
