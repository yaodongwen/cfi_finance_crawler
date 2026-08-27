import json

from crawl_framework.core.plugin import (
    CrawlCheckpoint,
    CrawlScope,
)
from crawl_framework.storage.checkpoint import (
    CheckpointKey,
    FileCheckpointStore,
    checkpoint_key_for_scope,
)


def test_checkpoint_key():

    key = CheckpointKey(
        site_id="naver_finance",
        dataset="forum_post",
        scope_type="instrument",
        source_key="005930",
    )

    assert (
        key.site_id
        == "naver_finance"
    )

    assert (
        key.source_key
        == "005930"
    )


def test_missing_checkpoint_returns_empty(
    tmp_path,
):

    store = FileCheckpointStore(
        tmp_path
        / "checkpoints"
    )

    key = CheckpointKey(
        site_id="naver_finance",
        dataset="forum_post",
        scope_type="instrument",
        source_key="005930",
    )

    checkpoint = store.load(
        key
    )

    assert (
        checkpoint.state
        == {}
    )


def test_save_and_load(
    tmp_path,
):

    store = FileCheckpointStore(
        tmp_path
        / "checkpoints"
    )

    key = CheckpointKey(
        site_id="tossinvest",
        dataset="forum_post",
        scope_type="instrument",
        source_key="A005930",
    )

    checkpoint = CrawlCheckpoint(
        state={
            "oldest_seen_id":
                "123456",

            "baseline_complete":
                False,

            "scroll_round":
                42,
        }
    )

    store.save(
        key,
        checkpoint,
    )

    loaded = store.load(
        key
    )

    assert (
        loaded.state
        == checkpoint.state
    )

    assert store.exists(
        key
    )


def test_checkpoint_file_structure(
    tmp_path,
):

    root = (
        tmp_path
        / "checkpoints"
    )

    store = FileCheckpointStore(
        root
    )

    key = CheckpointKey(
        site_id="naver_finance",
        dataset="news_article",
        scope_type="instrument",
        source_key="005930",
    )

    store.save(
        key,
        CrawlCheckpoint(
            state={
                "page": 100
            }
        ),
    )

    path = store.path_for(
        key
    )

    assert (
        path.name
        == "005930.json"
    )

    assert (
        "site=naver_finance"
        in path.as_posix()
    )

    assert (
        "dataset=news_article"
        in path.as_posix()
    )

    obj = json.loads(
        path.read_text(
            encoding="utf-8"
        )
    )

    assert (
        obj["state"]["page"]
        == 100
    )

    assert (
        obj["version"]
        == 1
    )


def test_different_sites_do_not_conflict(
    tmp_path,
):

    store = FileCheckpointStore(
        tmp_path
        / "checkpoints"
    )

    naver = CheckpointKey(
        site_id="naver_finance",
        dataset="forum_post",
        scope_type="instrument",
        source_key="005930",
    )

    toss = CheckpointKey(
        site_id="tossinvest",
        dataset="forum_post",
        scope_type="instrument",
        source_key="005930",
    )

    store.save(
        naver,
        CrawlCheckpoint(
            state={
                "page": 10
            }
        ),
    )

    store.save(
        toss,
        CrawlCheckpoint(
            state={
                "scroll": 20
            }
        ),
    )

    assert (
        store.path_for(
            naver
        )
        != store.path_for(
            toss
        )
    )

    assert (
        store.load(
            naver
        ).state["page"]
        == 10
    )

    assert (
        store.load(
            toss
        ).state["scroll"]
        == 20
    )


def test_delete(
    tmp_path,
):

    store = FileCheckpointStore(
        tmp_path
        / "checkpoints"
    )

    key = CheckpointKey(
        site_id="demo",
        dataset="news_article",
        scope_type="global",
        source_key="global",
    )

    store.save(
        key,
        CrawlCheckpoint(
            state={
                "cursor": "abc"
            }
        ),
    )

    assert store.exists(
        key
    )

    assert store.delete(
        key
    )

    assert not store.exists(
        key
    )

    assert (
        store.delete(
            key
        )
        is False
    )


def test_checkpoint_key_for_scope():

    scope = CrawlScope(
        scope_type="instrument",
        scope_id="XKRX:005930",
        source_key="A005930",
    )

    key = checkpoint_key_for_scope(
        site_id="tossinvest",
        dataset="forum_post",
        scope=scope,
    )

    assert (
        key.site_id
        == "tossinvest"
    )

    assert (
        key.source_key
        == "A005930"
    )


def test_special_source_key_is_safe(
    tmp_path,
):

    store = FileCheckpointStore(
        tmp_path
        / "checkpoints"
    )

    key = CheckpointKey(
        site_id="demo",
        dataset="forum_post",
        scope_type="instrument",
        source_key="../abc/123",
    )

    path = store.path_for(
        key
    )

    assert ".." not in path.name

    assert "/" not in path.name