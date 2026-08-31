from __future__ import annotations

import asyncio

from collections.abc import AsyncIterable, Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any


StageHandler = Callable[
    [
        Any,
    ],
    Awaitable[
        Any,
    ],
]


@dataclass(
    frozen=True,
    slots=True,
)
class PipelineStage:
    name: str

    worker_count: int

    queue_size: int

    handler: StageHandler

    def __post_init__(
        self,
    ) -> None:

        if not str(
            self.name
        ).strip():

            raise ValueError(
                "stage name cannot be empty"
            )

        if self.worker_count < 1:

            raise ValueError(
                "worker_count must be >= 1"
            )

        if self.queue_size < 1:

            raise ValueError(
                "queue_size must be >= 1"
            )


@dataclass(
    slots=True,
)
class PipelineRunStats:
    input_items: int = 0

    output_items: int = 0

    stage_counts: dict[
        str,
        int,
    ] | None = None

    max_queue_sizes: dict[
        str,
        int,
    ] | None = None

    def __post_init__(
        self,
    ) -> None:

        if self.stage_counts is None:

            self.stage_counts = {}

        if self.max_queue_sizes is None:

            self.max_queue_sizes = {}


class BoundedAsyncPipeline:
    """
    Generic bounded async stage pipeline.
    """

    def __init__(
        self,
        stages: Iterable[
            PipelineStage
        ],
    ) -> None:

        self.stages = tuple(
            stages
        )

        if not self.stages:

            raise ValueError(
                "at least one stage is required"
            )


    async def run(
        self,
        source: AsyncIterable[
            Any
        ],
    ) -> tuple[
        list[
            Any
        ],
        PipelineRunStats,
    ]:

        stats = PipelineRunStats()

        queues = [
            asyncio.Queue(
                maxsize=stage.queue_size
            )
            for stage in self.stages
        ]

        outputs: list[
            Any
        ] = []

        sentinel = object()

        finished_counts = [
            0
            for _ in self.stages
        ]

        finished_lock = asyncio.Lock()

        async def remember_size(
            name,
            queue,
        ) -> None:

            assert stats.max_queue_sizes is not None

            stats.max_queue_sizes[
                name
            ] = max(
                stats.max_queue_sizes.get(
                    name,
                    0,
                ),
                queue.qsize(),
            )

        async def producer() -> None:

            async for item in source:

                await queues[0].put(
                    item
                )

                stats.input_items += 1

                await remember_size(
                    self.stages[0].name,
                    queues[0],
                )

            for _ in range(
                self.stages[0].worker_count
            ):

                await queues[0].put(
                    sentinel
                )

        async def worker(
            index: int,
            stage: PipelineStage,
        ) -> None:

            input_queue = queues[
                index
            ]

            output_queue = (
                queues[
                    index
                    +
                    1
                ]
                if index + 1 < len(
                    queues
                )
                else None
            )

            while True:

                item = await input_queue.get()

                try:

                    if item is sentinel:

                        async with finished_lock:

                            finished_counts[
                                index
                            ] += 1

                            stage_finished = (
                                finished_counts[
                                    index
                                ]
                                ==
                                stage.worker_count
                            )

                        if (
                            stage_finished
                            and output_queue is not None
                        ):

                            next_stage = self.stages[
                                index
                                +
                                1
                            ]

                            for _ in range(
                                next_stage.worker_count
                            ):

                                await output_queue.put(
                                    sentinel
                                )

                        return

                    result = await stage.handler(
                        item
                    )

                    assert stats.stage_counts is not None

                    stats.stage_counts[
                        stage.name
                    ] = (
                        stats.stage_counts.get(
                            stage.name,
                            0,
                        )
                        +
                        1
                    )

                    result_items = _normalize_stage_result(
                        result
                    )

                    if output_queue is None:

                        outputs.extend(
                            result_items
                        )

                        stats.output_items += len(
                            result_items
                        )

                    else:

                        for result_item in result_items:

                            await output_queue.put(
                                result_item
                            )

                            await remember_size(
                                self.stages[
                                    index
                                    +
                                    1
                                ].name,
                                output_queue,
                            )

                finally:

                    input_queue.task_done()

        tasks = [
            asyncio.create_task(
                producer()
            )
        ]

        for index, stage in enumerate(
            self.stages
        ):

            for _ in range(
                stage.worker_count
            ):

                tasks.append(
                    asyncio.create_task(
                        worker(
                            index,
                            stage,
                        )
                    )
                )

        await asyncio.gather(
            *tasks
        )

        return (
            outputs,
            stats,
        )


def _normalize_stage_result(
    result,
) -> tuple:

    if result is None:

        return ()

    if isinstance(
        result,
        tuple,
    ):

        return result

    if isinstance(
        result,
        list,
    ):

        return tuple(
            result
        )

    return (
        result,
    )
