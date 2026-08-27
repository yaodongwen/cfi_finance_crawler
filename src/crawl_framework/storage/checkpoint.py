from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlScope,
)


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


def safe_component(
    value: str | None,
) -> str:
    """
    把 site/dataset/scope 转成安全路径组件。

    不追求可逆，只保证：
    - 不含 /
    - 不含 ..
    - 不为空
    """
    text = str(
        value or ""
    ).strip()

    if not text:
        return "_"

    result = []

    for char in text:
        if (
            char.isalnum()
            or char in {
                "-",
                "_",
                ".",
                ":",
            }
        ):
            result.append(
                char
            )
        else:
            result.append(
                "_"
            )

    cleaned = "".join(
        result
    )

    while ".." in cleaned:
        cleaned = cleaned.replace(
            "..",
            "_",
        )

    return cleaned or "_"


@dataclass(
    frozen=True,
    slots=True,
)
class CheckpointKey:
    """
    唯一 checkpoint 定位键。

    一个 checkpoint 唯一对应：

        site
        +
        dataset
        +
        scope_type
        +
        source_key

    注意：

    这里使用 source_key，
    而不是 scope_id。

    因为网页翻页/滚动状态通常是针对网站原始对象的。

    示例：

        Naver:
            source_key=005930

        Toss:
            source_key=A005930
    """

    site_id: str
    dataset: str
    scope_type: str
    source_key: str

    def __post_init__(
        self,
    ) -> None:

        for field_name in (
            "site_id",
            "dataset",
            "scope_type",
            "source_key",
        ):

            value = str(
                getattr(
                    self,
                    field_name,
                )
            ).strip()

            if not value:
                raise ValueError(
                    f"{field_name} "
                    "cannot be empty"
                )

            object.__setattr__(
                self,
                field_name,
                value,
            )


class CheckpointStore:
    """
    CheckpointStore 抽象接口。

    Pipeline / Runtime 不应该依赖具体文件实现。
    """

    def load(
        self,
        key: CheckpointKey,
    ) -> CrawlCheckpoint:
        raise NotImplementedError

    def save(
        self,
        key: CheckpointKey,
        checkpoint: CrawlCheckpoint,
    ) -> None:
        raise NotImplementedError

    def delete(
        self,
        key: CheckpointKey,
    ) -> bool:
        raise NotImplementedError

    def exists(
        self,
        key: CheckpointKey,
    ) -> bool:
        raise NotImplementedError


class FileCheckpointStore(
    CheckpointStore
):
    """
    基于 JSON 文件的 checkpoint store。

    目录：

        state/checkpoints/
            site=naver_finance/
                dataset=forum_post/
                    scope=instrument/
                        005930.json

    写入方式：

        temp file
            ↓
        flush
            ↓
        fsync
            ↓
        os.replace

    因此不会因为进程崩溃留下半个 JSON。
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
        key: CheckpointKey,
    ) -> Path:

        return (
            self.root
            / (
                "site="
                + safe_component(
                    key.site_id
                )
            )
            / (
                "dataset="
                + safe_component(
                    key.dataset
                )
            )
            / (
                "scope="
                + safe_component(
                    key.scope_type
                )
            )
            / (
                safe_component(
                    key.source_key
                )
                + ".json"
            )
        )


    def load(
        self,
        key: CheckpointKey,
    ) -> CrawlCheckpoint:

        path = self.path_for(
            key
        )

        if not path.exists():
            return CrawlCheckpoint()

        try:

            obj = json.loads(
                path.read_text(
                    encoding="utf-8"
                )
            )

        except Exception as exc:
            raise RuntimeError(
                "checkpoint read failed: "
                f"{path}"
            ) from exc

        if not isinstance(
            obj,
            dict,
        ):
            raise RuntimeError(
                "invalid checkpoint root: "
                f"{path}"
            )

        state = obj.get(
            "state",
            {},
        )

        if not isinstance(
            state,
            dict,
        ):
            raise RuntimeError(
                "checkpoint state must "
                f"be dict: {path}"
            )

        return CrawlCheckpoint(
            state=state
        )


    def save(
        self,
        key: CheckpointKey,
        checkpoint: CrawlCheckpoint,
    ) -> None:

        if not isinstance(
            checkpoint,
            CrawlCheckpoint,
        ):
            raise TypeError(
                "checkpoint must be "
                "CrawlCheckpoint"
            )

        if not isinstance(
            checkpoint.state,
            dict,
        ):
            raise TypeError(
                "checkpoint.state "
                "must be dict"
            )

        path = self.path_for(
            key
        )

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        payload: dict[
            str,
            Any,
        ] = {
            "version": 1,

            "site_id":
                key.site_id,

            "dataset":
                key.dataset,

            "scope_type":
                key.scope_type,

            "source_key":
                key.source_key,

            "updated_at":
                utc_now_iso(),

            "state":
                checkpoint.state,
        }

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


    def delete(
        self,
        key: CheckpointKey,
    ) -> bool:

        path = self.path_for(
            key
        )

        if not path.exists():
            return False

        path.unlink()

        return True


    def exists(
        self,
        key: CheckpointKey,
    ) -> bool:

        return self.path_for(
            key
        ).exists()


def checkpoint_key_for_scope(
    *,
    site_id: str,
    dataset: str,
    scope: CrawlScope,
) -> CheckpointKey:
    """
    Runtime 后续通过这个方法，
    从 CrawlScope 生成 checkpoint key。
    """

    return CheckpointKey(
        site_id=site_id,
        dataset=dataset,
        scope_type=scope.scope_type,
        source_key=scope.source_key,
    )