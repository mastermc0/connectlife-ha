"""Tests for the statistics sources, coordinator fetch loop, and sensor."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from typing import cast

import pytest
from connectlife.api import AirDuctEnergy, EnergyConsumption, EnergyResult, LifeConnectAuthError
from connectlife.appliance import ConnectLifeAppliance
from homeassistant.util import dt as dt_util

from custom_components.connectlife.coordinator import (
    ConnectLifeStatisticsCoordinator,
    _AcceptedStatistics,
    _deserialize_accepted,
    _serialize_accepted,
    _status_snapshot,
)
from custom_components.connectlife.dictionaries import Dictionaries, Dictionary
from custom_components.connectlife.sensor import ConnectLifeStatisticsSensor
from custom_components.connectlife.statistics_sources import (
    AirDuctStatisticsSource,
    ConsumptionStatisticsSource,
    _as_float,
    _curve_today,
    enabled_sensors,
)


def _today_key() -> str:
    return dt_util.now().date().isoformat()


# -- pure helpers ----------------------------------------------------------


def test_as_float():
    assert _as_float(None) is None
    assert _as_float("1.5") == 1.5
    assert _as_float(2) == 2.0
    assert _as_float("not a number") is None


def test_curve_today_returns_value_for_today():
    curve = {_today_key(): "1.5", "2000-01-01": "9.9"}
    assert _curve_today(curve) == 1.5


def test_curve_today_missing_today_is_none():
    assert _curve_today({"2000-01-01": "9.9"}) is None


def test_curve_today_empty_or_none_is_none():
    assert _curve_today(None) is None
    assert _curve_today({}) is None


def test_curve_today_non_numeric_is_none():
    assert _curve_today({_today_key(): "n/a"}) is None


# -- enabled_sensors -------------------------------------------------------


def test_enabled_sensors_unknown_source_is_empty():
    assert enabled_sensors(None, {}) == []
    assert enabled_sensors("does_not_exist", {"daily_energy_kwh": True}) == []


def test_enabled_sensors_only_listed_true_are_created():
    assert [s.key for s in enabled_sensors("air_duct_energy", {"daily_energy_kwh": True})] == [
        "daily_energy_kwh"
    ]
    assert enabled_sensors("air_duct_energy", {"daily_energy_kwh": False}) == []
    assert enabled_sensors("air_duct_energy", {}) == []


def test_enabled_sensors_consumption_energy_and_water():
    both = enabled_sensors(
        "energy_consumption_curve",
        {"daily_energy_kwh": True, "daily_water_consumption": True},
    )
    assert [s.key for s in both] == ["daily_energy_kwh", "daily_water_consumption"]

    energy_only = enabled_sensors("energy_consumption_curve", {"daily_energy_kwh": True})
    assert [s.key for s in energy_only] == ["daily_energy_kwh"]


# -- coordinator fetch loop ------------------------------------------------


class _FakeApi:
    """Records calls; optionally raises per endpoint."""

    def __init__(self, *, air_duct=None, consumption=None, air_duct_exc=None):
        self._air_duct = air_duct
        self._consumption = consumption
        self._air_duct_exc = air_duct_exc
        self.air_duct_calls = 0
        self.consumption_calls = 0

    async def get_air_duct_energy(self, *_args):
        self.air_duct_calls += 1
        if self._air_duct_exc is not None:
            raise self._air_duct_exc
        return self._air_duct

    async def get_energy_consumption_curve(self, *_args):
        self.consumption_calls += 1
        return self._consumption


def _appliance(
    device_id: str, type_code: str, feature: str, status_list: dict | None = None
) -> ConnectLifeAppliance:
    return cast(
        ConnectLifeAppliance,
        SimpleNamespace(
            device_id=device_id,
            device_type_code=type_code,
            device_feature_code=feature,
            puid=f"puid{device_id}",
            device_nickname=f"dev-{device_id}",
            status_list=status_list if status_list is not None else {},
        ),
    )


class _FakeStore:
    """Records saved payloads; ``preload`` seeds what ``async_load`` returns once."""

    def __init__(self, preload: dict | None = None):
        self.preload = preload
        self.saved: list[dict] = []

    async def async_load(self):
        return self.preload

    async def async_save(self, data):
        self.saved.append(data)


def _coordinator(api, data: dict, store: "_FakeStore | None" = None):
    # Bypass DataUpdateCoordinator.__init__ (needs hass); the fetch loop only uses
    # self.api and self.appliance_coordinator.data.
    coord = ConnectLifeStatisticsCoordinator.__new__(ConnectLifeStatisticsCoordinator)
    coord.api = api  # type: ignore[assignment]
    coord.appliance_coordinator = SimpleNamespace(data=data)  # type: ignore[assignment]
    coord._accepted = {}  # type: ignore[attr-defined]
    coord._accepted_dirty = False  # type: ignore[attr-defined]
    coord._store = store if store is not None else _FakeStore()  # type: ignore[assignment]
    return coord


# Synthetic device codes, seeded with statistics dictionaries below so the
# coordinator's source lookup doesn't depend on any shipped mapping file.
_AC = ("ac_type", "ac_feat")        # air_duct_energy
_WM = ("wm_type", "wm_feat")        # energy_consumption_curve
_NO_STATS = ("plain_type", "plain")  # no statistics endpoint


def _dictionary(source, sensors):
    return Dictionary(
        climate=None,
        properties={},
        buttons=[],
        statistics_source=source,
        statistics_sensors=sensors,
    )


@pytest.fixture(autouse=True)
def _seed_dictionaries():
    Dictionaries.dictionaries.clear()
    Dictionaries.dictionaries[f"{_AC[0]}-{_AC[1]}"] = _dictionary(
        "air_duct_energy", {"daily_energy_kwh": True}
    )
    Dictionaries.dictionaries[f"{_WM[0]}-{_WM[1]}"] = _dictionary(
        "energy_consumption_curve",
        {"daily_energy_kwh": True, "daily_water_consumption": True},
    )
    Dictionaries.dictionaries[f"{_NO_STATS[0]}-{_NO_STATS[1]}"] = _dictionary(None, {})
    yield
    Dictionaries.dictionaries.clear()


async def test_coordinator_stores_results_per_device():
    api = _FakeApi(
        air_duct=SimpleNamespace(electric_total=2.0),
        consumption=SimpleNamespace(
            electric_curve={_today_key(): "1.0"}, water_curve={_today_key(): "11.0"}
        ),
    )
    data = {"ac": _appliance("ac", *_AC), "wm": _appliance("wm", *_WM)}
    result = await _coordinator(api, data)._async_update_data()

    assert set(result) == {"ac", "wm"}
    assert result["ac"] is not None and result["ac"].electric_total == 2.0
    assert api.air_duct_calls == 1
    assert api.consumption_calls == 1


async def test_coordinator_auth_error_breaks_remaining_devices():
    # AC fetched first and raises auth error -> loop breaks before the washing machine.
    api = _FakeApi(air_duct_exc=LifeConnectAuthError("token rejected"))
    data = {"ac": _appliance("ac", *_AC), "wm": _appliance("wm", *_WM)}
    result = await _coordinator(api, data)._async_update_data()

    assert result == {}
    assert api.air_duct_calls == 1
    assert api.consumption_calls == 0  # never reached


async def test_coordinator_generic_error_yields_none_and_continues():
    api = _FakeApi(
        air_duct_exc=ValueError("boom"),
        consumption=SimpleNamespace(
            electric_curve={_today_key(): "1.0"}, water_curve={}
        ),
    )
    data = {"ac": _appliance("ac", *_AC), "wm": _appliance("wm", *_WM)}
    result = await _coordinator(api, data)._async_update_data()

    assert result["ac"] is None  # generic error -> None, not a break
    assert result["wm"] is not None and result["wm"].electric_curve  # later device still fetched
    assert api.consumption_calls == 1


async def test_coordinator_skips_device_without_statistics_source():
    api = _FakeApi()
    data = {"x": _appliance("x", *_NO_STATS)}  # dictionary has no statistics source
    result = await _coordinator(api, data)._async_update_data()

    assert result == {}
    assert api.air_duct_calls == 0
    assert api.consumption_calls == 0


# -- issue #669: suppress cloud replay of a completed cycle -----------------
#
# ConnectLife's statistics endpoints have been observed to replay a completed
# cycle's totals into "today" on an hourly cadence while the appliance sits
# idle. Since the client does no local accumulation, this would otherwise
# surface directly as a jump in the daily sensor. The coordinator mitigates
# this by only accepting a freshly fetched result when the day has rolled
# over or the appliance's status_list has changed since the last accepted
# reading (a proxy for "something actually happened").


def _consumption_result(today_kwh: str) -> EnergyResult:
    return cast(
        EnergyResult, SimpleNamespace(electric_curve={_today_key(): today_kwh}, water_curve={})
    )


def test_accept_or_reuse_first_reading_is_accepted():
    coord = _coordinator(_FakeApi(), {})
    appliance = _appliance("wm", *_WM, status_list={"machine_status": 1})
    today = dt_util.now().date()

    accepted = coord._accept_or_reuse("wm", appliance, today, _consumption_result("1.0"))

    assert accepted is not None
    assert _curve_today(accepted.electric_curve) == 1.0


def test_accept_or_reuse_suppresses_replay_when_status_unchanged():
    coord = _coordinator(_FakeApi(), {})
    appliance = _appliance("wm", *_WM, status_list={"machine_status": 1})  # idle/standby
    today = dt_util.now().date()

    first = coord._accept_or_reuse("wm", appliance, today, _consumption_result("26.0"))
    # Cloud replays the same completed cycle an hour later; nothing on the
    # device itself changed.
    second = coord._accept_or_reuse("wm", appliance, today, _consumption_result("28.0"))

    assert first is not None
    assert _curve_today(first.electric_curve) == 26.0
    assert second is first  # frozen: the replayed 28.0 is not surfaced
    assert second is not None
    assert _curve_today(second.electric_curve) == 26.0


def test_accept_or_reuse_accepts_when_status_changes():
    coord = _coordinator(_FakeApi(), {})
    today = dt_util.now().date()
    idle = _appliance("wm", *_WM, status_list={"machine_status": 1})
    running = _appliance("wm", *_WM, status_list={"machine_status": 2})  # a new cycle starts

    coord._accept_or_reuse("wm", idle, today, _consumption_result("26.0"))
    accepted = coord._accept_or_reuse("wm", running, today, _consumption_result("28.0"))

    assert accepted is not None
    assert _curve_today(accepted.electric_curve) == 28.0


def test_accept_or_reuse_accepts_on_new_day_even_if_status_unchanged():
    coord = _coordinator(_FakeApi(), {})
    appliance = _appliance("wm", *_WM, status_list={"machine_status": 1})
    day1 = date(2024, 1, 1)
    day2 = date(2024, 1, 2)

    coord._accept_or_reuse("wm", appliance, day1, _consumption_result("26.0"))
    accepted = coord._accept_or_reuse("wm", appliance, day2, _consumption_result("0.5"))

    assert accepted is not None
    assert _curve_today(accepted.electric_curve) == 0.5


def test_accept_or_reuse_clears_state_when_fetch_returns_none():
    coord = _coordinator(_FakeApi(), {})
    appliance = _appliance("wm", *_WM, status_list={"machine_status": 1})
    today = dt_util.now().date()

    coord._accept_or_reuse("wm", appliance, today, _consumption_result("26.0"))
    result = coord._accept_or_reuse("wm", appliance, today, None)
    assert result is None

    # A later successful fetch with unchanged status is treated as a fresh
    # baseline, not compared against the pre-None accepted value.
    accepted = coord._accept_or_reuse("wm", appliance, today, _consumption_result("99.0"))
    assert accepted is not None
    assert _curve_today(accepted.electric_curve) == 99.0


async def test_async_update_data_suppresses_replay_across_polls():
    api = _FakeApi(consumption=_consumption_result("26.0"))
    data = {"wm": _appliance("wm", *_WM, status_list={"machine_status": 1})}
    coord = _coordinator(api, data)

    first = await coord._async_update_data()
    api._consumption = _consumption_result("28.0")  # replay, device still idle
    second = await coord._async_update_data()

    assert first["wm"] is not None
    assert _curve_today(first["wm"].electric_curve) == 26.0
    assert second["wm"] is not None
    assert _curve_today(second["wm"].electric_curve) == 26.0


# -- persistence across restarts --------------------------------------------
#
# self._accepted only lives in memory, so a Home Assistant restart would
# otherwise leave the coordinator with no baseline: the first poll after every
# restart would blindly accept whatever the cloud currently reports, replay or
# not. Persisting accepted readings (and restoring them in _async_setup) closes
# that gap.


def _real_consumption_result(today_kwh: str) -> EnergyConsumption:
    return EnergyConsumption(
        stat_type="week",
        date_start="2024-01-01",
        date_end="2024-01-07",
        electric_total=None,
        electric_curve={_today_key(): today_kwh},
        raw={},
        water_total=None,
        run_time=None,
        cycles=None,
        norm_electric_total=None,
        norm_water_total=None,
        water_curve={},
        energy_period=None,
    )


def test_status_snapshot_stringifies_values():
    appliance = _appliance("wm", *_WM, status_list={"machine_status": 1, "door": "closed"})
    assert _status_snapshot(appliance) == {"machine_status": "1", "door": "closed"}


def test_serialize_deserialize_accepted_round_trips():
    accepted = _AcceptedStatistics(
        day=date(2024, 1, 1),
        status_snapshot={"machine_status": "1"},
        result=_real_consumption_result("26.0"),
    )

    restored = _deserialize_accepted(_serialize_accepted(accepted))

    assert restored is not None
    assert restored.day == accepted.day
    assert restored.status_snapshot == accepted.status_snapshot
    assert restored.result == accepted.result


def test_deserialize_accepted_discards_malformed_entries():
    assert _deserialize_accepted({}) is None
    assert _deserialize_accepted({"result_type": "not_a_real_type"}) is None
    assert _deserialize_accepted({"day": "not-a-date", "result_type": "EnergyConsumption"}) is None


async def test_async_setup_restores_accepted_state_and_suppresses_replay():
    accepted = _AcceptedStatistics(
        day=dt_util.now().date(),
        status_snapshot=_status_snapshot(_appliance("wm", *_WM, status_list={"machine_status": 1})),
        result=_real_consumption_result("26.0"),
    )
    store = _FakeStore(preload={"devices": {"wm": _serialize_accepted(accepted)}})
    api = _FakeApi(consumption=_real_consumption_result("28.0"))  # replay, on the "first" poll
    data = {"wm": _appliance("wm", *_WM, status_list={"machine_status": 1})}
    coord = _coordinator(api, data, store=store)

    await coord._async_setup()
    result = await coord._async_update_data()

    # Restored baseline means even the very first poll this run suppresses the replay.
    assert result["wm"] is not None
    assert _curve_today(result["wm"].electric_curve) == 26.0


async def test_async_update_data_persists_only_when_accepted_state_changes():
    store = _FakeStore()
    api = _FakeApi(consumption=_real_consumption_result("26.0"))
    data = {"wm": _appliance("wm", *_WM, status_list={"machine_status": 1})}
    coord = _coordinator(api, data, store=store)

    await coord._async_update_data()  # first reading: new baseline -> persists
    assert len(store.saved) == 1

    await coord._async_update_data()  # same status, replayed value -> suppressed, no I/O
    assert len(store.saved) == 1

    data["wm"] = _appliance("wm", *_WM, status_list={"machine_status": 2})  # real change
    await coord._async_update_data()
    assert len(store.saved) == 2


# -- sensor ----------------------------------------------------------------


class _FakeStatsCoordinator:
    def __init__(self, data, last_update_success=True):
        self.data = data
        self.last_update_success = last_update_success


def _energy_sensor_def():
    return ConsumptionStatisticsSource().sensors[0]  # daily_energy_kwh


def _make_sensor(coordinator, device_id="dev1", sensor_def=None):
    appliance_coordinator = SimpleNamespace(add_entity=lambda *a, **k: None)
    appliance = SimpleNamespace(device_id=device_id)
    return ConnectLifeStatisticsSensor(
        appliance_coordinator,  # type: ignore[arg-type]
        coordinator,  # type: ignore[arg-type]
        appliance,  # type: ignore[arg-type]
        sensor_def or _energy_sensor_def(),
    )


def test_sensor_extracts_value_and_is_available():
    coord = _FakeStatsCoordinator(
        {"dev1": SimpleNamespace(electric_curve={_today_key(): "1.5"})}
    )
    sensor = _make_sensor(coord)
    assert sensor.native_value == 1.5
    assert sensor.available is True


def test_sensor_air_duct_value():
    coord = _FakeStatsCoordinator({"dev1": SimpleNamespace(electric_total=2.0)})
    sensor = _make_sensor(coord, sensor_def=AirDuctStatisticsSource().sensors[0])
    assert sensor.native_value == 2.0
    assert sensor.available is True


def test_sensor_unavailable_when_result_none():
    coord = _FakeStatsCoordinator({"dev1": None})
    sensor = _make_sensor(coord)
    assert sensor.native_value is None
    assert sensor.available is False


def test_sensor_unavailable_when_device_missing():
    coord = _FakeStatsCoordinator({})
    sensor = _make_sensor(coord)
    assert sensor.native_value is None
    assert sensor.available is False


def test_sensor_unavailable_when_last_update_failed():
    coord = _FakeStatsCoordinator(
        {"dev1": SimpleNamespace(electric_curve={_today_key(): "1.5"})},
        last_update_success=False,
    )
    sensor = _make_sensor(coord)
    assert sensor.available is False
