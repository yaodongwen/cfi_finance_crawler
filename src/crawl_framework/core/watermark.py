from __future__ import annotations

from dataclasses import dataclass


@dataclass(
    frozen=True,
    slots=True,
)
class WatermarkAdvance:
    previous: int

    current: int

    advanced: bool


class ContiguousWatermark:
    """
    Tracks out-of-order task completion safely.
    """

    def __init__(
        self,
        *,
        start: int = 0,
    ) -> None:

        if start < 0:

            raise ValueError(
                "start must be >= 0"
            )

        self.current = start
        self._completed: set[
            int
        ] = set()


    def mark_complete(
        self,
        sequence: int,
    ) -> WatermarkAdvance:

        if sequence <= self.current:

            return WatermarkAdvance(
                previous=self.current,
                current=self.current,
                advanced=False,
            )

        previous = self.current

        self._completed.add(
            sequence
        )

        while (
            self.current
            +
            1
        ) in self._completed:

            self.current += 1

            self._completed.remove(
                self.current
            )

        return WatermarkAdvance(
            previous=previous,
            current=self.current,
            advanced=(
                self.current
                != previous
            ),
        )


    @property
    def pending_gap_count(
        self,
    ) -> int:

        return len(
            self._completed
        )
