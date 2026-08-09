from __future__ import annotations

import re
from collections.abc import Callable
from typing import Literal


TravelPreferenceValue = bool | Literal["balanced", "relaxed"]
TravelPreferenceParser = Callable[[str], dict[str, TravelPreferenceValue]]

AVOID_RED_EYE_STATE_KEY = "avoid_red_eye"
HOTEL_NEAR_SUBWAY_STATE_KEY = "hotel_near_subway"
TRAVEL_STYLE_STATE_KEY = "travel_style"


def parse_legacy_travel_preferences(
    user_message: str,
) -> dict[str, TravelPreferenceValue]:
    """Preserve the published ``travel-agent:1.0.0`` substring semantics."""

    text = user_message.lower()
    updates: dict[str, TravelPreferenceValue] = {}
    if "red-eye" in text or "red eye" in text or "红眼" in user_message:
        updates[AVOID_RED_EYE_STATE_KEY] = not any(
            phrase in text for phrase in ("allow red-eye", "allow red eye")
        )
    if "near subway" in text or "靠近地铁" in user_message:
        updates[HOTEL_NEAR_SUBWAY_STATE_KEY] = True
    if "relaxed" in text or "轻松" in user_message:
        updates[TRAVEL_STYLE_STATE_KEY] = "relaxed"
    return updates

_RED_EYE_PATTERNS: tuple[tuple[re.Pattern[str], bool], ...] = (
    (
        re.compile(
            r"\b(?:do not|don't|dont)\s+mind\s+(?:taking\s+)?(?:a\s+)?"
            r"red[- ]eye(?:\s+flights?)?\b"
        ),
        False,
    ),
    (
        re.compile(
            r"\b(?:allow|accept)\s+(?:taking\s+)?(?:a\s+)?"
            r"red[- ]eye(?:\s+flights?)?\b"
        ),
        False,
    ),
    (
        re.compile(
            r"\b(?:can|could|would)\s+(?:also\s+)?take\s+(?:a\s+)?"
            r"red[- ]eye(?:\s+flights?)?\b"
        ),
        False,
    ),
    (
        re.compile(
            r"\b(?:okay|ok|fine)\s+with\s+(?:taking\s+)?(?:a\s+)?"
            r"red[- ]eye(?:\s+flights?)?\b"
        ),
        False,
    ),
    (
        re.compile(
            r"\bred[- ]eye(?:\s+flights?)?\s+(?:are|is)\s+"
            r"(?:okay|ok|fine|acceptable)\b"
        ),
        False,
    ),
    (
        re.compile(
            r"\b(?:do not|don't|dont)\s+(?:want|like|prefer|take|book)\s+"
            r"(?:to\s+take\s+)?(?:a\s+)?red[- ]eye(?:\s+flights?)?\b"
        ),
        True,
    ),
    (
        re.compile(
            r"\bprefer\s+not\s+to\s+take\s+(?:a\s+)?"
            r"red[- ]eye(?:\s+flights?)?\b"
        ),
        True,
    ),
    (
        re.compile(
            r"\b(?:avoid|skip|refuse|hate|dislike)\s+(?:taking\s+)?(?:a\s+)?"
            r"red[- ]eye(?:\s+flights?)?\b"
        ),
        True,
    ),
    (re.compile(r"\bno\s+red[- ]eye(?:\s+flights?)?\b"), True),
    (re.compile(r"(?:可以|接受|不介意)(?:乘坐)?红眼(?:航班)?"), False),
    (re.compile(r"(?:避免|不要|不想|拒绝)(?:乘坐)?红眼(?:航班)?"), True),
)

_HOTEL_NEAR_SUBWAY_PATTERNS: tuple[tuple[re.Pattern[str], bool], ...] = (
    (
        re.compile(
            r"\b(?:do not|don't|dont)\s+(?:want|prefer|need|book|choose)\s+"
            r"(?:a\s+)?(?:hotel|accommodation)\s+"
            r"(?:that\s+is\s+)?near\s+(?:the\s+)?subway\b"
        ),
        False,
    ),
    (
        re.compile(
            r"\b(?:avoid|skip)\s+(?:a\s+)?(?:hotel|accommodation)\s+"
            r"near\s+(?:the\s+)?subway\b"
        ),
        False,
    ),
    (
        re.compile(
            r"\b(?:want|prefer|need|book|choose)\s+(?:a\s+)?"
            r"(?:hotel|accommodation)\s+(?:that\s+is\s+)?"
            r"not\s+near\s+(?:the\s+)?subway\b"
        ),
        False,
    ),
    (
        re.compile(
            r"\b(?:hotel|accommodation)\s+(?:should|must)\s+not\s+be\s+"
            r"near\s+(?:the\s+)?subway\b"
        ),
        False,
    ),
    (
        re.compile(
            r"(?<!not )(?<!don't )(?<!dont )\b"
            r"(?:want|prefer|need|book|choose)\s+(?:a\s+)?"
            r"(?:hotel|accommodation)\s+(?:that\s+is\s+)?"
            r"near\s+(?:the\s+)?subway\b"
        ),
        True,
    ),
    (
        re.compile(
            r"\b(?:hotel|accommodation)\s+(?:should|must)\s+be\s+"
            r"near\s+(?:the\s+)?subway\b"
        ),
        True,
    ),
    (
        re.compile(r"\bprefer\s+to\s+stay\s+near\s+(?:the\s+)?subway\b"),
        True,
    ),
    (
        re.compile(r"(?:不想|不要|不需要|避免).{0,12}(?:酒店|住宿).{0,8}靠近地铁"),
        False,
    ),
    (
        re.compile(r"(?:想要|希望|偏好|喜欢|需要).{0,12}(?:酒店|住宿).{0,8}靠近地铁"),
        True,
    ),
)

_TRAVEL_STYLE_PATTERNS: tuple[tuple[re.Pattern[str], Literal["balanced", "relaxed"]], ...] = (
    (
        re.compile(
            r"\b(?:do not|don't|dont)\s+(?:want|prefer|like|choose)\s+"
            r"(?:a\s+)?relaxed(?:\s+(?:travel|trip|itinerary)(?:\s+style)?)?\b"
        ),
        "balanced",
    ),
    (
        re.compile(
            r"\bprefer\s+not\s+(?:a\s+)?relaxed"
            r"(?:\s+(?:travel|trip|itinerary)(?:\s+style)?)?\b"
        ),
        "balanced",
    ),
    (
        re.compile(
            r"\bavoid\s+(?:a\s+)?relaxed"
            r"(?:\s+(?:travel|trip|itinerary)(?:\s+style)?)?\b"
        ),
        "balanced",
    ),
    (
        re.compile(
            r"\b(?:want|prefer|like|choose)\s+(?:a\s+)?balanced"
            r"(?:\s+(?:travel|trip|itinerary)(?:\s+style)?)?\b"
        ),
        "balanced",
    ),
    (
        re.compile(
            r"(?<!not )(?<!don't )(?<!dont )\b"
            r"(?:want|prefer|like|choose)\s+(?:a\s+)?relaxed"
            r"(?:\s+(?:travel|trip|itinerary)(?:\s+style)?)?\b"
        ),
        "relaxed",
    ),
    (
        re.compile(r"\b(?:keep|make)\s+(?:the\s+)?(?:travel|trip|itinerary)\s+relaxed\b"),
        "relaxed",
    ),
    (re.compile(r"(?:不想|不要|不喜欢|避免).{0,8}(?:轻松|悠闲)(?:旅行|行程|风格)?"), "balanced"),
    (re.compile(r"(?:想要|希望|偏好|喜欢).{0,8}(?:轻松|悠闲)(?:旅行|行程|风格)?"), "relaxed"),
    (re.compile(r"(?:想要|希望|偏好|喜欢).{0,8}(?:均衡|平衡)(?:旅行|行程|风格)?"), "balanced"),
)


def parse_explicit_travel_preferences(
    user_message: str,
) -> dict[str, TravelPreferenceValue]:
    """Parse only explicit, stable preference intent from one user message."""

    text = " ".join(user_message.lower().replace("’", "'").split())
    updates: dict[str, TravelPreferenceValue] = {}

    red_eye = _last_explicit_value(text, _RED_EYE_PATTERNS)
    if isinstance(red_eye, bool):
        updates[AVOID_RED_EYE_STATE_KEY] = red_eye

    hotel_near_subway = _last_explicit_value(text, _HOTEL_NEAR_SUBWAY_PATTERNS)
    if isinstance(hotel_near_subway, bool):
        updates[HOTEL_NEAR_SUBWAY_STATE_KEY] = hotel_near_subway

    travel_style = _last_explicit_value(text, _TRAVEL_STYLE_PATTERNS)
    if isinstance(travel_style, str):
        updates[TRAVEL_STYLE_STATE_KEY] = travel_style

    return updates


def _last_explicit_value(
    text: str,
    patterns: tuple[tuple[re.Pattern[str], TravelPreferenceValue], ...],
) -> TravelPreferenceValue | None:
    matches = (
        (match.start(), match.end(), value)
        for pattern, value in patterns
        for match in pattern.finditer(text)
    )
    latest = max(matches, default=None, key=lambda candidate: candidate[:2])
    return None if latest is None else latest[2]
