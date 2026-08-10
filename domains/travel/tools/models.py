from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, model_validator


class SearchTripOptionsInput(BaseModel):
    """Deterministic reference-catalog search inputs for Travel 1.0."""

    model_config = ConfigDict(extra="forbid")

    destination: str = Field(min_length=1, max_length=200)
    days: int = Field(gt=0, le=365)
    avoid_red_eye: bool
    hotel_near_subway: bool
    travel_style: Literal["balanced", "relaxed"]


class SearchTripOption(BaseModel):
    """One synthetic option returned by the offline Travel reference tool."""

    model_config = ConfigDict(extra="forbid")

    index: int = Field(ge=0)
    name: str = Field(min_length=1, max_length=200)
    cost: float = Field(ge=0, le=10_000_000)
    duration_hours: float = Field(gt=0, le=10_000)
    transport_cost: int = Field(ge=0, le=1_000_000)
    hotel_cost: int = Field(ge=0, le=1_000_000)
    activity_cost: int = Field(ge=0, le=1_000_000)
    flight_type: Literal["daytime", "red_eye"]
    hotel_tier: str = Field(min_length=1, max_length=200)
    poi_style: str = Field(min_length=1, max_length=200)


class SearchTripOptionsResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: Literal["synthetic_reference_catalog"]
    destination: str
    options: list[SearchTripOption]


class RouteCostInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transport_cost: int = Field(ge=0, le=1_000_000)
    hotel_cost: int = Field(ge=0, le=1_000_000)
    activity_cost: int = Field(ge=0, le=1_000_000)
    budget: int = Field(gt=0, le=10_000_000)


class RouteCostResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_cost: int
    budget: int
    remaining_budget: int
    within_budget: bool


class TripOptionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    cost: float = Field(ge=0, le=10_000_000)
    duration_hours: float = Field(gt=0, le=10_000)


class RankOptionsInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    options: list[TripOptionInput] = Field(min_length=1, max_length=50)
    # Preserve the Phase 4 HTTP/tool-call contract. The model-provider adapter
    # independently normalizes strict schemas so model calls must still supply
    # every property explicitly.
    cost_weight: float = Field(default=0.6, ge=0, le=1)
    duration_weight: float = Field(default=0.4, ge=0, le=1)


class RankedTripOption(BaseModel):
    model_config = ConfigDict(extra="forbid")

    index: int = Field(ge=0)
    name: str
    score: float


class RankOptionsResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ranking: list[RankedTripOption]


class CreateTripHoldInput(BaseModel):
    """Typed request sent to the Travel reference hold provider."""

    model_config = ConfigDict(extra="forbid")

    destination: str = Field(min_length=1, max_length=200)
    selected_option_name: str = Field(min_length=1, max_length=200)
    quoted_total: int = Field(ge=0, le=10_000_000)
    hold_minutes: int = Field(default=15, ge=1, le=24 * 60)


class CreateTripHoldResult(BaseModel):
    """Sanitized provider evidence allowed into durable/public Travel state."""

    model_config = ConfigDict(extra="forbid")

    status: Literal["held"]
    provider_reference: str = Field(
        pattern=r"^hold_[A-Za-z0-9_-]{1,128}$",
        max_length=133,
    )
    destination: str = Field(min_length=1, max_length=200)
    selected_option_name: str = Field(min_length=1, max_length=200)
    quoted_total: int = Field(ge=0, le=10_000_000)
    hold_minutes: int = Field(ge=1, le=24 * 60)

    @model_validator(mode="after")
    def matches_prepared_arguments(self, info: ValidationInfo) -> "CreateTripHoldResult":
        context = info.context
        if not isinstance(context, dict):
            return self
        arguments = context.get("arguments")
        if not isinstance(arguments, dict):
            return self
        expected = {
            "destination": arguments.get("destination"),
            "selected_option_name": arguments.get("selected_option_name"),
            "quoted_total": arguments.get("quoted_total"),
            "hold_minutes": arguments.get("hold_minutes"),
        }
        actual = {
            "destination": self.destination,
            "selected_option_name": self.selected_option_name,
            "quoted_total": self.quoted_total,
            "hold_minutes": self.hold_minutes,
        }
        if actual != expected:
            raise ValueError("Trip hold result does not match prepared arguments")
        return self
