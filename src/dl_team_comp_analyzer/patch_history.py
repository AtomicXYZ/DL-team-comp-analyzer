from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime


@dataclass(frozen=True)
class PatchRelease:
    patch_name: str
    effective_from_utc: datetime
    source: str


PATCH_RELEASES: tuple[PatchRelease, ...] = (
    PatchRelease(
        patch_name="2026-04-10",
        effective_from_utc=datetime(2026, 4, 10, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2026-03-25",
        effective_from_utc=datetime(2026, 3, 25, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2026-03-21",
        effective_from_utc=datetime(2026, 3, 21, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2026-03-06",
        effective_from_utc=datetime(2026, 3, 6, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2026-01-30",
        effective_from_utc=datetime(2026, 1, 30, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-12-29",
        effective_from_utc=datetime(2025, 12, 29, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-12-16",
        effective_from_utc=datetime(2025, 12, 16, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-11-21",
        effective_from_utc=datetime(2025, 11, 21, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-10-24",
        effective_from_utc=datetime(2025, 10, 24, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-10-02",
        effective_from_utc=datetime(2025, 10, 2, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-09-04",
        effective_from_utc=datetime(2025, 9, 4, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-08-18",
        effective_from_utc=datetime(2025, 8, 18, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-07-29",
        effective_from_utc=datetime(2025, 7, 29, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-07-04",
        effective_from_utc=datetime(2025, 7, 4, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-06-17",
        effective_from_utc=datetime(2025, 6, 17, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-05-27",
        effective_from_utc=datetime(2025, 5, 27, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-05-21",
        effective_from_utc=datetime(2025, 5, 21, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-05-19",
        effective_from_utc=datetime(2025, 5, 19, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-05-11",
        effective_from_utc=datetime(2025, 5, 11, tzinfo=UTC),
        source="official gameplay update date",
    ),
    PatchRelease(
        patch_name="2025-05-08",
        effective_from_utc=datetime(2025, 5, 8, tzinfo=UTC),
        source="official gameplay update date",
    ),
)


def infer_patch_from_start_time(start_time_s: int | None) -> tuple[str, str]:
    if start_time_s is None:
        return "Unknown", "missing start_time"

    match_time = datetime.fromtimestamp(start_time_s, tz=UTC)
    selected = None
    for patch in PATCH_RELEASES:
        if match_time >= patch.effective_from_utc:
            selected = patch
            break

    if selected is None:
        return "Unknown", "no known patch release before match time"

    return selected.patch_name, f"inferred from start_time ({selected.source})"
