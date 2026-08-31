from __future__ import annotations

import json
import re

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


CANONICAL_INSTRUMENT_RE = re.compile(
    r"^[A-Z0-9]+:[A-Z0-9._-]+$"
)


@dataclass(
    frozen=True,
    slots=True,
)
class UniverseSnapshot:
    instrument_ids: tuple[
        str,
        ...
    ]

    metadata: dict


def validate_canonical_instrument_id(
    instrument_id: str,
) -> None:

    if not CANONICAL_INSTRUMENT_RE.fullmatch(
        instrument_id
    ):

        raise ValueError(
            "not a canonical instrument_id: "
            f"{instrument_id!r}"
        )


def deduplicate_instruments(
    instrument_ids: Sequence[
        str
    ],
) -> tuple[
    str,
    ...
]:

    result: list[
        str
    ] = []

    seen: set[
        str
    ] = set()

    for value in instrument_ids:

        instrument_id = str(
            value
        ).strip()

        if not instrument_id:

            continue

        validate_canonical_instrument_id(
            instrument_id
        )

        if instrument_id in seen:

            continue

        seen.add(
            instrument_id
        )

        result.append(
            instrument_id
        )

    return tuple(
        result
    )


def stage_instruments(
    instrument_ids: Sequence[
        str
    ],
    count: int,
) -> tuple[
    str,
    ...
]:

    universe = deduplicate_instruments(
        instrument_ids
    )

    if len(
        universe
    ) < count:

        raise ValueError(
            f"universe has {len(universe)} instruments; "
            f"requires at least {count}"
        )

    return universe[
        :count
    ]


def validate_rollout_prefixes(
    instrument_ids: Sequence[
        str
    ],
) -> None:

    universe = deduplicate_instruments(
        instrument_ids
    )

    if len(
        universe
    ) >= 50:

        g1 = stage_instruments(
            universe,
            50,
        )

        if len(
            g1
        ) != 50:

            raise AssertionError(
                "G1 must contain exactly 50 instruments"
            )

    if len(
        universe
    ) >= 500:

        g1 = stage_instruments(
            universe,
            50,
        )

        g2 = stage_instruments(
            universe,
            500,
        )

        if len(
            g2
        ) != 500:

            raise AssertionError(
                "G2 must contain exactly 500 instruments"
            )

        if g2[
            :50
        ] != g1:

            raise AssertionError(
                "G1 must be the G2 prefix"
            )

        if tuple(
            universe[
                :500
            ]
        ) != g2:

            raise AssertionError(
                "G2 must be the full-universe prefix"
            )


def write_universe_snapshot(
    path: str | Path,
    instrument_ids: Sequence[
        str
    ],
    *,
    metadata: dict,
) -> UniverseSnapshot:

    destination = Path(
        path
    )

    universe = deduplicate_instruments(
        instrument_ids
    )

    validate_rollout_prefixes(
        universe
    )

    snapshot_metadata = dict(
        metadata
    )

    snapshot_metadata.setdefault(
        "generated_at",
        datetime.now(
            timezone.utc
        ).isoformat(),
    )

    snapshot_metadata[
        "count"
    ] = len(
        universe
    )

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    lines = [
        "# crawl_framework rollout universe snapshot",
        "# metadata: "
        + json.dumps(
            snapshot_metadata,
            ensure_ascii=False,
            sort_keys=True,
        ),
        *universe,
        "",
    ]

    destination.write_text(
        "\n".join(
            lines
        ),
        encoding="utf-8",
    )

    return UniverseSnapshot(
        instrument_ids=universe,
        metadata=snapshot_metadata,
    )


def read_universe_snapshot(
    path: str | Path,
) -> UniverseSnapshot:

    source = Path(
        path
    )

    metadata: dict = {}

    values: list[
        str
    ] = []

    for line in source.read_text(
        encoding="utf-8",
    ).splitlines():

        stripped = line.strip()

        if not stripped:

            continue

        if stripped.startswith(
            "# metadata:"
        ):

            metadata = json.loads(
                stripped.split(
                    ":",
                    1,
                )[1].strip()
            )

            continue

        if stripped.startswith(
            "#"
        ):

            continue

        values.append(
            stripped
        )

    universe = deduplicate_instruments(
        values
    )

    if (
        metadata
        and metadata.get(
            "count"
        )
        != len(
            universe
        )
    ):

        raise ValueError(
            "snapshot metadata count does not match body"
        )

    validate_rollout_prefixes(
        universe
    )

    return UniverseSnapshot(
        instrument_ids=universe,
        metadata=metadata,
    )
