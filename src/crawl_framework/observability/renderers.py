from __future__ import annotations

import importlib.util
import sys
from typing import TextIO

from crawl_framework.observability.progress import PlatformProgressSnapshot


def rich_is_available() -> bool:
    return importlib.util.find_spec("rich") is not None


def select_progress_style(
    style: str,
    *,
    is_tty: bool,
    rich_available: bool,
) -> str:
    value = str(style).strip().lower()
    if value not in {"auto", "rich", "text"}:
        raise ValueError(f"unsupported progress style: {style!r}")
    if value == "text":
        return "text"
    if value == "rich":
        return "rich" if rich_available else "text"
    return "rich" if is_tty and rich_available else "text"


def _bar(percent: float, width: int = 20) -> str:
    filled = min(width, max(0, round(width * percent / 100.0)))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


class TextProgressRenderer:
    def __init__(self, stream: TextIO = sys.stderr) -> None:
        self.stream = stream

    def render(self, snapshot: PlatformProgressSnapshot) -> None:
        eta = (
            f"{snapshot.eta_seconds:.0f}s"
            if snapshot.eta_seconds is not None
            else "--"
        )
        print(
            "[progress] "
            f"profile={snapshot.profile or '-'} "
            f"status={snapshot.status.value} "
            f"elapsed={snapshot.elapsed_seconds:.1f}s "
            f"scopes={snapshot.completed_scopes}/{snapshot.total_scopes} "
            f"percent={snapshot.percent:.1f}% "
            f"remaining={snapshot.remaining_scopes} eta={eta} "
            f"recovery=pending:{snapshot.recovery.pending},"
            f"recovered:{snapshot.recovery.recovered},"
            f"terminal:{snapshot.recovery.terminal_failed}",
            file=self.stream,
        )
        for site in snapshot.sites:
            print(
                f"[site] {site.site_id} status={site.status.value} "
                f"scopes={site.completed_scopes}/{site.total_scopes} "
                f"percent={site.percent:.1f}%",
                file=self.stream,
            )
            for dataset in site.datasets:
                print(
                    f"[dataset] {site.site_id}/{dataset.dataset} "
                    f"{_bar(dataset.percent)} "
                    f"{dataset.completed_scopes}/{dataset.total_scopes} "
                    f"{dataset.percent:.1f}% "
                    f"running={dataset.running_scopes} "
                    f"skipped={dataset.skipped_due_to_checkpoint} "
                    f"blocked={dataset.blocked_scopes} "
                    f"failed={dataset.failed_scopes} "
                    f"records={dataset.records.crawled}/"
                    f"{dataset.records.normalized} "
                    f"new={dataset.records.new} "
                    f"unchanged={dataset.records.unchanged} "
                    f"files={dataset.storage.files_written}/"
                    f"{dataset.storage.files_uploaded}/"
                    f"{dataset.storage.files_cataloged} "
                    f"queues={dataset.queues.record_current}/"
                    f"{dataset.queues.record_max},"
                    f"{dataset.queues.upload_current}/"
                    f"{dataset.queues.upload_max},"
                    f"{dataset.queues.catalog_current}/"
                    f"{dataset.queues.catalog_max}",
                    file=self.stream,
                )
                print(
                    f"[storage] {site.site_id}/{dataset.dataset} "
                    f"buffered={dataset.storage.buffered_rows} "
                    f"prepared={dataset.storage.files_prepared} "
                    f"verified={dataset.storage.files_verified} "
                    f"active={dataset.storage.active_files} "
                    f"compacted={dataset.storage.files_compacted} "
                    f"replacements={dataset.storage.replacement_files} "
                    f"superseded={dataset.storage.source_files_superseded} "
                    f"recovery=pending:{dataset.recovery_pending_scopes},"
                    f"retryable:{dataset.recovery.retryable_failed},"
                    f"terminal:{dataset.recovery.terminal_failed}",
                    file=self.stream,
                )
                print(
                    f"[performance] {site.site_id}/{dataset.dataset} "
                    f"records_per_sec="
                    f"{dataset.performance.records_per_second:.2f} "
                    f"files_per_min="
                    f"{dataset.performance.files_per_minute:.2f} "
                    f"upload_mb_per_sec="
                    f"{dataset.performance.upload_mb_per_second:.2f} "
                    f"busy=crawl:{dataset.performance.crawl_busy_time:.2f},"
                    f"write:{dataset.performance.write_busy_time:.2f},"
                    f"upload:{dataset.performance.upload_busy_time:.2f},"
                    f"catalog:{dataset.performance.catalog_busy_time:.2f}",
                    file=self.stream,
                )
        self.stream.flush()

    def close(self) -> None:
        return None


class RichProgressRenderer:
    def __init__(self, stream: TextIO = sys.stderr) -> None:
        from rich.console import Console
        from rich.live import Live

        self._console = Console(file=stream)
        self._live = Live(
            console=self._console,
            refresh_per_second=4,
            transient=False,
        )
        self._started = False

    def render(self, snapshot: PlatformProgressSnapshot) -> None:
        table = self._build_table(snapshot)
        if not self._started:
            self._live.start(refresh=True)
            self._started = True
        self._live.update(table, refresh=True)

    def close(self) -> None:
        if self._started:
            self._live.stop()
            self._started = False

    @staticmethod
    def _build_table(snapshot: PlatformProgressSnapshot):
        from rich.table import Table

        title = (
            f"{snapshot.profile or 'crawl'} | {snapshot.status.value} | "
            f"{snapshot.completed_scopes}/{snapshot.total_scopes} "
            f"({snapshot.percent:.1f}%)"
        )
        table = Table(title=title, expand=True)
        table.add_column("Site / Dataset", overflow="fold", ratio=2)
        table.add_column("Status")
        table.add_column("Scopes", justify="right")
        table.add_column("Progress", justify="right")
        table.add_column("Metrics", overflow="fold", ratio=3)
        for site in snapshot.sites:
            for dataset in site.datasets:
                table.add_row(
                    f"{site.site_id} / {dataset.dataset}",
                    dataset.status.value,
                    f"{dataset.completed_scopes}/{dataset.total_scopes}",
                    f"{dataset.percent:.1f}%",
                    (
                        f"rec {dataset.records.crawled}/"
                        f"{dataset.records.normalized} | files "
                        f"{dataset.storage.files_written}/"
                        f"{dataset.storage.files_uploaded}/"
                        f"{dataset.storage.files_cataloged} | q "
                        f"{dataset.queues.record_current}/"
                        f"{dataset.queues.upload_current}/"
                        f"{dataset.queues.catalog_current} | skip "
                        f"{dataset.skipped_due_to_checkpoint} | "
                        f"{dataset.performance.records_per_second:.1f} rec/s"
                    ),
                )
        return table


def create_progress_renderer(
    *,
    style: str,
    stream: TextIO = sys.stderr,
    is_tty: bool | None = None,
):
    tty = bool(stream.isatty()) if is_tty is None else bool(is_tty)
    resolved = select_progress_style(
        style,
        is_tty=tty,
        rich_available=rich_is_available(),
    )
    if resolved == "rich":
        return RichProgressRenderer(stream)
    return TextProgressRenderer(stream)
