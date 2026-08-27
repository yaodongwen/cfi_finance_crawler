from __future__ import annotations

from typing import (
    Any,
    AsyncIterator,
)

import pytest

from crawl_framework.core.models import (
    CanonicalRecord,
)
from crawl_framework.core.pipeline import (
    PipelineRecordDecision,
)
from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlContext,
    CrawlScope,
    SitePlugin,
)
from crawl_framework.core.runtime import (
    CrawlRuntime,
    make_scope_token,
    run_runtime,
)
from crawl_framework.storage.checkpoint import (
    FileCheckpointStore,
    checkpoint_key_for_scope,
)
from crawl_framework.storage.seen_store import (
    SQLiteSeenStore,
)


# ============================================================
# Fake Pipeline
# ============================================================


class FakePipeline:
    """
    Runtime 专用 FakePipeline。

    模拟：

        submit(scope_token)
        flush_scope(scope_token)
        SeenStore durability

    pending 保存：

        (
            record,
            scope_token,
        )

    这样可以验证某个 scope barrier
    不会 flush 其他 scope。
    """

    def __init__(
        self,
        seen_store,
    ) -> None:

        self.records = []

        self.pending = []

        self.flush_count = 0

        self.scope_flush_count = 0

        self.flush_all_count = 0

        self.flushed_scope_tokens = []

        self.seen_store = (
            seen_store
        )


    def submit(
        self,
        record: CanonicalRecord,
        *,
        scope_token: str | None = None,
    ):

        self.records.append(
            record
        )

        seen = (
            self.seen_store
            .inspect(
                record.record_uid,
                record.version_hash,
            )
        )

        if (
            seen.decision
            != "unchanged"
        ):

            self.pending.append(
                (
                    record,
                    scope_token,
                )
            )

        return (
            PipelineRecordDecision(
                record_uid=(
                    record.record_uid
                ),
                version_hash=(
                    record.version_hash
                ),
                decision=(
                    seen.decision
                ),
                buffered=(
                    seen.decision
                    != "unchanged"
                ),
            ),
            [],
        )


    def flush_scope(
        self,
        scope_token: str,
    ):

        self.flush_count += 1

        self.scope_flush_count += 1

        self.flushed_scope_tokens.append(
            scope_token
        )

        matched = []

        remaining = []

        for (
            record,
            token,
        ) in self.pending:

            if token == scope_token:

                matched.append(
                    record
                )

            else:

                remaining.append(
                    (
                        record,
                        token,
                    )
                )

        rows = [
            (
                record.record_uid,
                record.version_hash,
            )
            for record
            in matched
        ]

        self.seen_store.commit_many(
            rows
        )

        self.pending = remaining

        return [
            object()
            for _ in range(
                1 if matched else 0
            )
        ]


    def flush_all(
        self,
    ):

        self.flush_count += 1

        self.flush_all_count += 1

        rows = [
            (
                record.record_uid,
                record.version_hash,
            )
            for (
                record,
                _
            )
            in self.pending
        ]

        self.seen_store.commit_many(
            rows
        )

        count = len(
            self.pending
        )

        self.pending.clear()

        return [
            object()
            for _ in range(
                1 if count else 0
            )
        ]


def make_fake_pipeline(
    tmp_path,
):

    seen_store = SQLiteSeenStore(
        tmp_path
        / "state"
        / "seen.sqlite3"
    )

    return FakePipeline(
        seen_store
    )


# ============================================================
# Demo Plugin
# ============================================================


class DemoPlugin(
    SitePlugin
):

    @property
    def site_id(
        self,
    ) -> str:

        return "demo_site"


    @property
    def country(
        self,
    ) -> str:

        return "KR"


    @property
    def timezone(
        self,
    ) -> str:

        return "Asia/Seoul"


    def datasets(
        self,
    ):

        return [
            "forum_post"
        ]


    async def discover(
        self,
        dataset: str,
        ctx: CrawlContext,
    ) -> AsyncIterator[
        CrawlScope
    ]:

        yield CrawlScope(
            scope_type="instrument",
            scope_id="XKRX:005930",
            source_key="005930",
        )

        yield CrawlScope(
            scope_type="instrument",
            scope_id="XKRX:000660",
            source_key="000660",
        )


    async def crawl(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> AsyncIterator[
        dict[str, Any]
    ]:

        start_page = int(
            checkpoint.state.get(
                "page",
                0,
            )
        )

        for index in range(
            2
        ):

            page = (
                start_page
                + index
                + 1
            )

            yield {
                "id": (
                    f"{scope.source_key}-"
                    f"{page}"
                ),

                "title": (
                    f"title-{page}"
                ),
            }

            checkpoint.state[
                "page"
            ] = page


    def normalize(
        self,
        dataset: str,
        raw: dict[str, Any],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:

        return CanonicalRecord(
            site_id=self.site_id,
            country=self.country,
            dataset=dataset,
            source_id=raw["id"],
            scope_type=scope.scope_type,
            scope_id=scope.scope_id,
            instrument_id=scope.scope_id,
            title=raw.get(
                "title"
            ),
        )


# ============================================================
# Skip Plugin
# ============================================================


class SkipPlugin(
    DemoPlugin
):

    def normalize(
        self,
        dataset: str,
        raw: dict[str, Any],
        scope: CrawlScope,
    ) -> CanonicalRecord | None:

        if raw["id"].endswith(
            "-1"
        ):

            return None

        return super().normalize(
            dataset,
            raw,
            scope,
        )


# ============================================================
# Error Plugin
# ============================================================


class ErrorPlugin(
    DemoPlugin
):

    async def crawl(
        self,
        dataset: str,
        scope: CrawlScope,
        checkpoint: CrawlCheckpoint,
        ctx: CrawlContext,
    ) -> AsyncIterator[
        dict[str, Any]
    ]:

        checkpoint.state[
            "page"
        ] = 999

        yield {
            "id": "first",
            "title": "first",
        }

        raise RuntimeError(
            "crawler failed"
        )


# ============================================================
# Runtime Tests
# ============================================================


@pytest.mark.asyncio
async def test_run_dataset(
    tmp_path,
):

    plugin = DemoPlugin()

    pipeline = make_fake_pipeline(
        tmp_path
    )

    checkpoint_store = (
        FileCheckpointStore(
            tmp_path
            / "checkpoints"
        )
    )

    runtime = CrawlRuntime(
        plugin=plugin,
        pipeline=pipeline,
        checkpoint_store=(
            checkpoint_store
        ),
    )

    results = (
        await runtime.run_dataset(
            "forum_post"
        )
    )

    assert (
        len(results)
        == 2
    )

    assert (
        len(pipeline.records)
        == 4
    )

    assert (
        runtime.stats.scopes_finished
        == 2
    )

    assert (
        runtime.stats.raw_records
        == 4
    )


@pytest.mark.asyncio
async def test_checkpoint_saved(
    tmp_path,
):

    plugin = DemoPlugin()

    pipeline = make_fake_pipeline(
        tmp_path
    )

    store = FileCheckpointStore(
        tmp_path
        / "checkpoints"
    )

    runtime = CrawlRuntime(
        plugin=plugin,
        pipeline=pipeline,
        checkpoint_store=store,
    )

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    result = await runtime.run_scope(
        "forum_post",
        scope,
    )

    assert (
        result.checkpoint_state[
            "page"
        ]
        == 2
    )

    key = checkpoint_key_for_scope(
        site_id="demo_site",
        dataset="forum_post",
        scope=scope,
    )

    loaded = store.load(
        key
    )

    assert (
        loaded.state[
            "page"
        ]
        == 2
    )


@pytest.mark.asyncio
async def test_checkpoint_resume(
    tmp_path,
):

    plugin = DemoPlugin()

    pipeline = make_fake_pipeline(
        tmp_path
    )

    store = FileCheckpointStore(
        tmp_path
        / "checkpoints"
    )

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    key = checkpoint_key_for_scope(
        site_id="demo_site",
        dataset="forum_post",
        scope=scope,
    )

    store.save(
        key,
        CrawlCheckpoint(
            state={
                "page": 10
            }
        ),
    )

    runtime = CrawlRuntime(
        plugin=plugin,
        pipeline=pipeline,
        checkpoint_store=store,
    )

    result = await runtime.run_scope(
        "forum_post",
        scope,
    )

    assert (
        result.checkpoint_state[
            "page"
        ]
        == 12
    )

    assert (
        pipeline.records[0]
        .source_id
        == "005930-11"
    )


@pytest.mark.asyncio
async def test_normalize_none_skipped(
    tmp_path,
):

    plugin = SkipPlugin()

    pipeline = make_fake_pipeline(
        tmp_path
    )

    runtime = CrawlRuntime(
        plugin=plugin,
        pipeline=pipeline,
        checkpoint_store=(
            FileCheckpointStore(
                tmp_path
                / "checkpoints"
            )
        ),
    )

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    result = await runtime.run_scope(
        "forum_post",
        scope,
    )

    assert (
        result.raw_count
        == 2
    )

    assert (
        result.normalized_count
        == 1
    )

    assert (
        result.skipped_count
        == 1
    )


@pytest.mark.asyncio
async def test_failed_scope_does_not_save_checkpoint(
    tmp_path,
):

    plugin = ErrorPlugin()

    pipeline = make_fake_pipeline(
        tmp_path
    )

    store = FileCheckpointStore(
        tmp_path
        / "checkpoints"
    )

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    key = checkpoint_key_for_scope(
        site_id="demo_site",
        dataset="forum_post",
        scope=scope,
    )

    store.save(
        key,
        CrawlCheckpoint(
            state={
                "page": 5
            }
        ),
    )

    runtime = CrawlRuntime(
        plugin=plugin,
        pipeline=pipeline,
        checkpoint_store=store,
    )

    with pytest.raises(
        RuntimeError
    ):

        await runtime.run_scope(
            "forum_post",
            scope,
        )

    loaded = store.load(
        key
    )

    # 原 checkpoint 不应被 999 覆盖
    assert (
        loaded.state[
            "page"
        ]
        == 5
    )


@pytest.mark.asyncio
async def test_run_flushes_pipeline(
    tmp_path,
):

    runtime = CrawlRuntime(
        plugin=DemoPlugin(),
        pipeline=make_fake_pipeline(
            tmp_path
        ),
        checkpoint_store=(
            FileCheckpointStore(
                tmp_path
                / "checkpoints"
            )
        ),
    )

    await runtime.run()

    assert (
        runtime.pipeline.scope_flush_count
        == 2
    )

    assert (
        runtime.pipeline.flush_all_count
        == 1
    )

    assert (
        runtime.pipeline.flush_count
        == 3
    )

@pytest.mark.asyncio
async def test_run_without_final_flush(
    tmp_path,
):

    runtime = CrawlRuntime(
        plugin=DemoPlugin(),
        pipeline=make_fake_pipeline(
            tmp_path
        ),
        checkpoint_store=(
            FileCheckpointStore(
                tmp_path
                / "checkpoints"
            )
        ),
    )

    await runtime.run(
        flush_at_end=False
    )

    assert (
        runtime.pipeline.scope_flush_count
        == 2
    )

    assert (
        runtime.pipeline.flush_all_count
        == 0
    )

    assert (
        runtime.pipeline.flush_count
        == 2
    )

@pytest.mark.asyncio
async def test_unknown_dataset_fails(
    tmp_path,
):

    runtime = CrawlRuntime(
        plugin=DemoPlugin(),
        pipeline=make_fake_pipeline(
            tmp_path
        ),
        checkpoint_store=(
            FileCheckpointStore(
                tmp_path
                / "checkpoints"
            )
        ),
    )

    with pytest.raises(
        ValueError
    ):

        await runtime.run_dataset(
            "comment"
        )


def test_sync_run_runtime(
    tmp_path,
):

    runtime = CrawlRuntime(
        plugin=DemoPlugin(),
        pipeline=make_fake_pipeline(
            tmp_path
        ),
        checkpoint_store=(
            FileCheckpointStore(
                tmp_path
                / "checkpoints"
            )
        ),
    )

    results = run_runtime(
        runtime
    )

    assert (
        len(results)
        == 2
    )

    assert (
        len(
            runtime.pipeline.records
        )
        == 4
    )


# ============================================================
# Barrier + Checkpoint durability tests
# ============================================================


@pytest.mark.asyncio
async def test_checkpoint_saved_only_after_durable(
    tmp_path,
):

    plugin = DemoPlugin()

    pipeline = make_fake_pipeline(
        tmp_path
    )

    store = FileCheckpointStore(
        tmp_path
        / "checkpoints"
    )

    runtime = CrawlRuntime(
        plugin=plugin,
        pipeline=pipeline,
        checkpoint_store=store,
    )

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    await runtime.run_scope(
        "forum_post",
        scope,
    )

    # Runtime 必须触发 barrier flush
    assert (
        pipeline.flush_count
        == 1
    )

    # 两条记录都应该 durable
    assert (
        pipeline.seen_store.count()
        == 2
    )

    key = checkpoint_key_for_scope(
        site_id="demo_site",
        dataset="forum_post",
        scope=scope,
    )

    checkpoint = store.load(
        key
    )

    assert (
        checkpoint.state[
            "page"
        ]
        == 2
    )


# ============================================================
# Broken durability pipeline
# ============================================================

class BrokenDurabilityPipeline(
    FakePipeline
):
    """
    模拟 scope storage durability 失败。

    flush_scope 被调用，
    但不 commit SeenStore。
    """

    def flush_scope(
        self,
        scope_token: str,
    ):

        self.flush_count += 1

        self.scope_flush_count += 1

        self.flushed_scope_tokens.append(
            scope_token
        )

        return []


@pytest.mark.asyncio
async def test_checkpoint_not_saved_when_barrier_fails(
    tmp_path,
):

    seen_store = SQLiteSeenStore(
        tmp_path
        / "state"
        / "seen.sqlite3"
    )

    pipeline = BrokenDurabilityPipeline(
        seen_store
    )

    plugin = DemoPlugin()

    store = FileCheckpointStore(
        tmp_path
        / "checkpoints"
    )

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    key = checkpoint_key_for_scope(
        site_id="demo_site",
        dataset="forum_post",
        scope=scope,
    )

    store.save(
        key,
        CrawlCheckpoint(
            state={
                "page": 10
            }
        ),
    )

    runtime = CrawlRuntime(
        plugin=plugin,
        pipeline=pipeline,
        checkpoint_store=store,
    )

    with pytest.raises(
        RuntimeError
    ):

        await runtime.run_scope(
            "forum_post",
            scope,
        )

    # 原 checkpoint 必须保持在 10。
    loaded = store.load(
        key
    )

    assert (
        loaded.state[
            "page"
        ]
        == 10
    )

    # Storage 没有 durable。
    assert (
        seen_store.count()
        == 0
    )

def test_make_scope_token():

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    token = make_scope_token(
        site_id="naver_finance",
        dataset="forum_post",
        scope=scope,
    )

    assert (
        token
        == (
            "naver_finance"
            "|forum_post"
            "|instrument"
            "|005930"
        )
    )


@pytest.mark.asyncio
async def test_scope_result_contains_scope_token(
    tmp_path,
):

    runtime = CrawlRuntime(
        plugin=DemoPlugin(),
        pipeline=make_fake_pipeline(
            tmp_path
        ),
        checkpoint_store=(
            FileCheckpointStore(
                tmp_path
                / "checkpoints"
            )
        ),
    )

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    result = await runtime.run_scope(
        "forum_post",
        scope,
    )

    assert (
        result.scope_token
        == (
            "demo_site"
            "|forum_post"
            "|instrument"
            "|005930"
        )
    )


@pytest.mark.asyncio
async def test_runtime_uses_scope_flush(
    tmp_path,
):

    pipeline = make_fake_pipeline(
        tmp_path
    )

    runtime = CrawlRuntime(
        plugin=DemoPlugin(),
        pipeline=pipeline,
        checkpoint_store=(
            FileCheckpointStore(
                tmp_path
                / "checkpoints"
            )
        ),
    )

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="005930",
    )

    await runtime.run_scope(
        "forum_post",
        scope,
    )

    assert (
        pipeline.scope_flush_count
        == 1
    )

    assert (
        pipeline.flush_all_count
        == 0
    )

    assert (
        pipeline.flushed_scope_tokens
        == [
            (
                "demo_site"
                "|forum_post"
                "|instrument"
                "|005930"
            )
        ]
    )


@pytest.mark.asyncio
async def test_two_scopes_have_different_tokens(
    tmp_path,
):

    pipeline = make_fake_pipeline(
        tmp_path
    )

    runtime = CrawlRuntime(
        plugin=DemoPlugin(),
        pipeline=pipeline,
        checkpoint_store=(
            FileCheckpointStore(
                tmp_path
                / "checkpoints"
            )
        ),
    )

    results = await runtime.run_dataset(
        "forum_post"
    )

    tokens = {
        result.scope_token
        for result
        in results
    }

    assert tokens == {
        (
            "demo_site"
            "|forum_post"
            "|instrument"
            "|005930"
        ),
        (
            "demo_site"
            "|forum_post"
            "|instrument"
            "|000660"
        ),
    }

    assert (
        pipeline.scope_flush_count
        == 2
    )

    assert (
        pipeline.flush_all_count
        == 0
    )