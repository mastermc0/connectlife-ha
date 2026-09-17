import async_timeout
import logging
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

from connectlife.api import (
    AirDuctEnergy,
    ConnectLifeApi,
    EnergyConsumption,
    EnergyResult,
    LifeConnectAuthError,
    LifeConnectError,
)
from connectlife.appliance import ConnectLifeAppliance
from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.statistics import list_statistic_ids
from homeassistant.const import Platform
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers import device_registry as dr, entity_registry as er, issue_registry as ir
from homeassistant.helpers.storage import Store
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import DATA_STATE_CLASS_MIGRATION_DONE, DOMAIN
from .dictionaries import Dictionaries
from .messages import format_retry_message
from .statistics_sources import STATISTICS_SOURCES, enabled_sensors

MAX_RETRIES = 3
STATISTICS_UPDATE_INTERVAL = timedelta(minutes=10)
STATISTICS_ACCEPTED_STORAGE_VERSION = 1

# EnergyResult subclasses, keyed by class name, for restoring persisted statistics.
_ENERGY_RESULT_TYPES: dict[str, type[EnergyResult]] = {
    cls.__name__: cls for cls in (AirDuctEnergy, EnergyConsumption)
}

_LOGGER = logging.getLogger(__name__)


class ConnectLifeCoordinator(DataUpdateCoordinator[dict[str, ConnectLifeAppliance]]):
    """ConnectLife coordinator."""

    # We need initial data, so no retries for first request.
    error_count = MAX_RETRIES

    def __init__(self, hass, api: ConnectLifeApi):
        """Initialize coordinator."""
        self.api = api
        # Register of entities created this setup, keyed by unique ID (used by
        # cleanup_removed_entities). Instance-scoped so a reload — e.g. after
        # toggling a per-device option — starts fresh and prunes entities that
        # are no longer created, such as the offline-state binary sensor.
        self.entities: dict[str, Platform] = {}
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=60),
        )

    async def _async_update_data(self):
        """Fetch data from API endpoint."""
        try:
            # Note: aiohttp.ClientError is already handled by the data update
            # coordinator. TimeoutError is retried here so the UI gets the
            # same user-facing retry message as other ConnectLife API errors.
            async with async_timeout.timeout(30):
                await self.api.get_appliances()
                self.error_count = 0
        except LifeConnectAuthError as err:
            # Raising ConfigEntryAuthFailed will cancel future updates
            # and start a config flow with SOURCE_REAUTH (async_step_reauth)
            raise ConfigEntryAuthFailed from err
        except TimeoutError as err:
            self.error_count += 1
            i = MAX_RETRIES - self.error_count
            if i > 0:
                _LOGGER.debug(
                    "ConnectLife API request timed out, will try %d more %s",
                    i,
                    "time" if i == 1 else "times",
                )
            else:
                raise UpdateFailed(format_retry_message(err)) from err
        except LifeConnectError as err:
            self.error_count += 1
            i = MAX_RETRIES - self.error_count
            if i > 0:
                _LOGGER.debug(
                    "ConnectLife API failed with '%s', will try %d more %s",
                    err,
                    i,
                    "time" if i == 1 else "times",
                )
            else:
                raise UpdateFailed(format_retry_message(err)) from err
        return {a.device_id: a for a in self.api.appliances}

    async def async_update_device(self, device_id: str, command: Mapping[str, int | str], properties: Mapping[str, int | str]):
        """Updates the device, and sets the properties in local copy and notify to avoid refetching."""
        await self.api.update_appliance(self.data[device_id].puid, {k: str(v) for k, v in command.items()})
        self.data[device_id].status_list.update(properties)
        self.async_update_listeners()

    def add_entity(self, entity_unique_id: str, platform: Platform):
        """Add known entity."""
        self.entities[entity_unique_id] = platform;

    async def cleanup_removed_entities(self) -> None:
        """
        Cleanup entity registry for entities converted to a different entity
        type or set to disabled in the mapping file, and create issues for
        unavailable devices.
        """

        device_reg = dr.async_get(self.hass)
        entity_reg = er.async_get(self.hass)

        for entity in er.async_entries_for_config_entry(
                entity_reg, self.config_entry.entry_id
        ):
            if entity.unique_id not in self.entities or entity.domain != self.entities[entity.unique_id]:
                if entity.device_id is None:
                    continue
                device = device_reg.async_get(entity.device_id)
                if device is None:
                    continue
                for (domain, device_id) in device.identifiers:
                    if domain == DOMAIN and device_id in self.data:
                        _LOGGER.info(
                            "Entity %s (%s) is no longer mapped, removing",
                            entity.unique_id,
                            entity.domain
                        )
                        entity_reg.async_remove(entity.entity_id)

        for device in dr.async_entries_for_config_entry(device_reg, self.config_entry.entry_id):
            for (domain, device_id) in device.identifiers:
                if domain == DOMAIN:
                    if device_id not in self.data:
                        _LOGGER.warning("Unavailable device: %s", device.name)
                        ir.async_create_issue(
                            self.hass,
                            DOMAIN,
                            f"unavailable_device.{device_id}",
                            data={
                                "device_id": device.id,
                                "device_name": device.name,
                            },
                            is_fixable=True,
                            severity=ir.IssueSeverity.WARNING,
                            translation_key="unavailable_device",
                            translation_placeholders={
                                "device_name": device.name or "",
                            },
                        )
                    else:
                        # Self repair
                        ir.async_delete_issue(self.hass, DOMAIN, f"unavailable_device.{device_id}")

    async def find_orphaned_statistics(self) -> list[str]:
        """Return entity_ids of our sensors with stored LTS but no current ``state_class``.

        Sensors lose ``state_class`` when a property is remapped or when the
        old auto-default to ``measurement`` no longer applies. The recorder
        keeps the historical data and emits one repair per entity. Collect
        them so we can offer a single bulk-clear action.
        """
        entity_reg = er.async_get(self.hass)
        candidates: list[str] = []
        for appliance in self.data.values():
            dictionary = Dictionaries.get_dictionary(appliance)
            for name, prop in dictionary.properties.items():
                if not hasattr(prop, Platform.SENSOR):
                    continue
                if prop.sensor.state_class is not None:
                    continue
                unique_id = f"{appliance.device_id}-{name}"
                entity_id = entity_reg.async_get_entity_id(
                    Platform.SENSOR, DOMAIN, unique_id
                )
                if entity_id:
                    candidates.append(entity_id)

        if not candidates:
            return []

        recorder = get_instance(self.hass)
        metas = await recorder.async_add_executor_job(
            list_statistic_ids, self.hass, set(candidates)
        )
        return sorted(m["statistic_id"] for m in metas)

    async def update_orphaned_statistics_issue(self) -> None:
        """Create or clear the bulk repair issue for orphaned statistics.

        When no orphans are found the migration is effectively complete for
        this entry — fresh installs and lucky upgraders never had any to
        clean up. Mark the flag so we don't re-run detection on every
        future setup.
        """
        issue_id = f"orphaned_statistics.{self.config_entry.entry_id}"
        orphans = await self.find_orphaned_statistics()
        if not orphans:
            ir.async_delete_issue(self.hass, DOMAIN, issue_id)
            self.hass.config_entries.async_update_entry(
                self.config_entry,
                data={
                    **self.config_entry.data,
                    DATA_STATE_CLASS_MIGRATION_DONE: True,
                },
            )
            return
        ir.async_create_issue(
            self.hass,
            DOMAIN,
            issue_id,
            data={"entry_id": self.config_entry.entry_id},
            is_fixable=True,
            severity=ir.IssueSeverity.CRITICAL,
            translation_key="orphaned_statistics",
            translation_placeholders={"count": str(len(orphans))},
        )


@dataclass
class _AcceptedStatistics:
    """The last statistics result we chose to expose for a device, and the
    conditions under which it was accepted."""

    day: date
    status_snapshot: dict[str, Any]
    result: EnergyResult


class ConnectLifeStatisticsCoordinator(DataUpdateCoordinator[dict[str, EnergyResult | None]]):
    """ConnectLife statistics coordinator. Polls each appliance's statistics endpoint
    (selected per device type via the data dictionary ``statistics_source``) every 10
    minutes. Stores the fetched result per device; sensors extract their datapoint.

    ConnectLife's cloud statistics endpoints are known to occasionally replay a
    completed cycle's totals into "today" on an hourly cadence, even while the
    appliance sits idle (see
    https://github.com/oyvindwe/connectlife-ha/issues/669). Since the client performs
    no local accumulation, such a replay would otherwise show up directly as a jump
    in the daily sensor. To mitigate this without a way to tell a genuine reading
    from a replayed one, a freshly fetched result is only accepted when either the
    local day has rolled over, or the appliance's own status has changed since the
    last accepted reading — a proxy for "something actually happened". Otherwise the
    previously accepted result keeps being served. Legitimate ongoing activity (a
    running cycle) continuously changes status properties (phase, remaining time,
    etc.), so this should not suppress real increases.

    The accepted-reading state is persisted (see ``_async_setup``/``_async_save_accepted``)
    so a Home Assistant restart doesn't lose it — otherwise the first poll after every
    restart would have no baseline to compare against and would blindly accept whatever
    the cloud happens to be reporting at that moment, replay-inflated or not.
    """

    def __init__(
        self,
        hass,
        api: ConnectLifeApi,
        appliance_coordinator: ConnectLifeCoordinator,
        entry_id: str,
    ):
        """Initialize statistics coordinator."""
        self.api = api
        self.appliance_coordinator = appliance_coordinator
        self._accepted: dict[str, _AcceptedStatistics] = {}
        self._accepted_dirty = False
        self._store: Store[dict[str, Any]] = Store(
            hass, STATISTICS_ACCEPTED_STORAGE_VERSION, f"{DOMAIN}_{entry_id}_statistics_accepted"
        )
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_statistics",
            update_interval=STATISTICS_UPDATE_INTERVAL,
        )

    async def _async_setup(self) -> None:
        """Restore accepted statistics readings from a previous run, if any."""
        try:
            stored = await self._store.async_load()
        except Exception:
            _LOGGER.debug("Failed to load persisted statistics state", exc_info=True)
            return
        if not stored:
            return
        for device_id, entry in stored.get("devices", {}).items():
            accepted = _deserialize_accepted(entry)
            if accepted is not None:
                self._accepted[device_id] = accepted

    async def _async_save_accepted(self) -> None:
        """Persist the current accepted-reading state."""
        devices: dict[str, Any] = {}
        for device_id, accepted in self._accepted.items():
            try:
                devices[device_id] = _serialize_accepted(accepted)
            except TypeError:
                # accepted.result wasn't a plain dataclass instance; skip persisting
                # this one device rather than losing the whole save.
                _LOGGER.debug(
                    "Could not serialize statistics state for %s", device_id, exc_info=True
                )
        try:
            await self._store.async_save({"devices": devices})
        except Exception:
            _LOGGER.debug("Failed to persist statistics state", exc_info=True)

    async def _async_update_data(self) -> dict[str, EnergyResult | None]:
        """Fetch statistics for appliances whose data dictionary opts into an endpoint."""
        result: dict[str, EnergyResult | None] = {}
        today = dt_util.now().date()
        for device_id, appliance in self.appliance_coordinator.data.items():
            dictionary = Dictionaries.get_dictionary(appliance)
            source = STATISTICS_SOURCES.get(dictionary.statistics_source or "")
            if source is None or not enabled_sensors(
                dictionary.statistics_source, dictionary.statistics_sensors
            ):
                continue
            try:
                fetched = await source.fetch(self.api, appliance)
            except LifeConnectAuthError:
                # Token is rejected; stop rather than hammering the gateway (and any
                # re-login) for every remaining device. Recovers on the next cycle.
                _LOGGER.debug("Statistics auth failed; skipping remaining devices this cycle")
                break
            except Exception:
                _LOGGER.debug(
                    "Failed to fetch statistics for %s",
                    appliance.device_nickname,
                    exc_info=True,
                )
                result[device_id] = None
                if self._accepted.pop(device_id, None) is not None:
                    self._accepted_dirty = True
                continue
            result[device_id] = self._accept_or_reuse(device_id, appliance, today, fetched)
        if self._accepted_dirty:
            await self._async_save_accepted()
            self._accepted_dirty = False
        return result

    def _accept_or_reuse(
        self,
        device_id: str,
        appliance: ConnectLifeAppliance,
        today: date,
        fetched: EnergyResult | None,
    ) -> EnergyResult | None:
        """Decide whether to accept a freshly fetched result or keep serving the last one."""
        if fetched is None:
            if self._accepted.pop(device_id, None) is not None:
                self._accepted_dirty = True
            return None

        status_snapshot = _status_snapshot(appliance)
        accepted = self._accepted.get(device_id)
        if (
            accepted is not None
            and accepted.day == today
            and accepted.status_snapshot == status_snapshot
        ):
            _LOGGER.debug(
                "Suppressing statistics update for %s: no device status change since last "
                "accepted reading (see issue #669)",
                appliance.device_nickname,
            )
            return accepted.result

        self._accepted[device_id] = _AcceptedStatistics(today, status_snapshot, fetched)
        self._accepted_dirty = True
        return fetched


def _status_snapshot(appliance: ConnectLifeAppliance) -> dict[str, str]:
    """A plain-string snapshot of an appliance's status, for equality comparison.

    Stringifying keeps this both a valid equality-comparable snapshot and directly
    JSON-serializable (``status_list`` values may be ``datetime``), so the same
    representation is used for live comparisons and for persisted state.
    """
    return {k: str(v) for k, v in appliance.status_list.items()}


def _serialize_accepted(accepted: _AcceptedStatistics) -> dict[str, Any]:
    return {
        "day": accepted.day.isoformat(),
        "status_snapshot": accepted.status_snapshot,
        "result_type": type(accepted.result).__name__,
        "result": asdict(accepted.result),
    }


def _deserialize_accepted(entry: dict[str, Any]) -> _AcceptedStatistics | None:
    try:
        result_cls = _ENERGY_RESULT_TYPES[entry["result_type"]]
        return _AcceptedStatistics(
            day=date.fromisoformat(entry["day"]),
            status_snapshot=entry["status_snapshot"],
            result=result_cls(**entry["result"]),
        )
    except (KeyError, TypeError, ValueError):
        _LOGGER.debug("Discarding malformed persisted statistics entry", exc_info=True)
        return None
