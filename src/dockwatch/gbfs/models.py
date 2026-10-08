"""Typed GBFS 2.3 records. Unknown fields are kept (extra="allow") so a feed addition never breaks parsing."""

from pydantic import BaseModel, ConfigDict, Field


class _Record(BaseModel):
    model_config = ConfigDict(extra="allow")

    @property
    def extra_fields(self) -> dict:
        return dict(self.model_extra or {})


class Envelope(_Record):
    """The wrapper every GBFS feed shares."""

    last_updated: int
    ttl: int
    version: str | None = None
    data: dict


class VehicleTypeCount(_Record):
    vehicle_type_id: str
    count: int


class StationStatus(_Record):
    station_id: str
    num_bikes_available: int = Field(ge=0)
    num_docks_available: int = Field(ge=0)
    is_installed: bool
    is_renting: bool
    is_returning: bool
    last_reported: int
    num_bikes_disabled: int | None = None
    num_docks_disabled: int | None = None
    num_ebikes_available: int | None = None
    vehicle_types_available: list[VehicleTypeCount] | None = None


class StationInformation(_Record):
    station_id: str
    name: str
    lat: float
    lon: float
    short_name: str | None = None
    region_id: str | None = None
    capacity: int | None = None
    address: str | None = None


class SystemRegion(_Record):
    region_id: str
    name: str


def dedupe_regions(regions: list[SystemRegion]) -> list[SystemRegion]:
    """Bay Wheels lists every region twice in system_regions; keep the first of each region_id."""
    seen: dict[str, SystemRegion] = {}
    for region in regions:
        seen.setdefault(region.region_id, region)
    return list(seen.values())
