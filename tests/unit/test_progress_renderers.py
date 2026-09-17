from __future__ import annotations

from io import StringIO

import pytest

from crawl_framework.observability.progress import (
    DatasetProgressSnapshot,
    PerformanceProgress,
    PlatformProgressSnapshot,
    ProgressStatus,
    QueueProgress,
    RecordProgress,
    RecoveryProgress,
    SiteProgressSnapshot,
    StorageProgress,
)
from crawl_framework.observability.renderers import (
    TextProgressRenderer,
    RichProgressRenderer,
    create_progress_renderer,
    select_progress_style,
)


def snapshot() -> PlatformProgressSnapshot:
    dataset = DatasetProgressSnapshot(
        site_id="naver_finance",
        dataset="forum_post",
        status=ProgressStatus.RUNNING,
        total_scopes=10,
        completed_scopes=6,
        remaining_scopes=4,
        percent=60.0,
        running_scopes=2,
        skipped_scopes=5,
        skipped_due_to_checkpoint=5,
        blocked_scopes=0,
        failed_scopes=0,
        recovery_pending_scopes=0,
        records=RecordProgress(
            crawled=100,
            normalized=99,
            new=20,
            unchanged=78,
            updated=1,
            failed=1,
        ),
        storage=StorageProgress(
            buffered_rows=21,
            files_prepared=3,
            files_written=3,
            files_uploaded=2,
            files_verified=2,
            files_cataloged=1,
            files_compacted=1,
            source_files_superseded=4,
        ),
        queues=QueueProgress(
            record_current=2,
            record_max=8,
            upload_current=1,
            upload_max=3,
            catalog_current=0,
            catalog_max=2,
        ),
        recovery=RecoveryProgress(),
        performance=PerformanceProgress(
            records_per_second=10,
            files_per_minute=18,
            upload_mb_per_second=2.5,
            crawl_busy_time=4,
            write_busy_time=1,
            upload_busy_time=2,
            catalog_busy_time=0.5,
        ),
    )
    site = SiteProgressSnapshot(
        site_id="naver_finance",
        status=ProgressStatus.RUNNING,
        total_scopes=10,
        completed_scopes=6,
        remaining_scopes=4,
        percent=60.0,
        datasets=(dataset,),
    )
    return PlatformProgressSnapshot(
        profile="naver_full",
        elapsed_seconds=10,
        status=ProgressStatus.RUNNING,
        total_scopes=10,
        completed_scopes=6,
        remaining_scopes=4,
        percent=60.0,
        estimated_remaining_scopes=4,
        eta_seconds=8,
        sites=(site,),
        recovery=RecoveryProgress(attempted=1, recovered=1),
    )


def test_progress_style_selection() -> None:
    assert select_progress_style(
        "auto", is_tty=True, rich_available=True
    ) == "rich"
    assert select_progress_style(
        "auto", is_tty=False, rich_available=True
    ) == "text"
    assert select_progress_style(
        "auto", is_tty=True, rich_available=False
    ) == "text"
    assert select_progress_style(
        "rich", is_tty=True, rich_available=False
    ) == "text"
    assert select_progress_style(
        "text", is_tty=True, rich_available=True
    ) == "text"


def test_text_renderer_contains_resume_and_stage_metrics() -> None:
    stream = StringIO()
    renderer = TextProgressRenderer(stream)

    renderer.render(snapshot())

    output = stream.getvalue()
    assert "profile=naver_full" in output
    assert "scopes=6/10" in output
    assert "naver_finance/forum_post" in output
    assert "[############--------]" in output
    assert "skipped=5" in output
    assert "records=100/99" in output
    assert "files=3/2/1" in output
    assert "queues=2/8,1/3,0/2" in output
    assert "compacted=1" in output
    assert "superseded=4" in output
    assert "records_per_sec=10.00" in output
    assert "upload_mb_per_sec=2.50" in output


def test_create_auto_non_tty_renderer_is_text() -> None:
    renderer = create_progress_renderer(
        style="auto",
        stream=StringIO(),
        is_tty=False,
    )

    assert isinstance(renderer, TextProgressRenderer)


def test_rich_live_renderer_smoke() -> None:
    pytest.importorskip("rich")
    stream = StringIO()
    renderer = RichProgressRenderer(stream)

    renderer.render(snapshot())
    renderer.close()

    output = stream.getvalue()
    assert "naver_finance" in output
    assert "forum_post" in output
