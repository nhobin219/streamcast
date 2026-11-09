"""A shared host grants stream names and storage profiles independently."""

from dataclasses import replace

import pytest

from streamcast import Storage

from .test_broker import config as config
from .test_broker import definition, managed, put


class TenantGrants:
    def __init__(self):
        self.creations = []

    async def authenticate(self, scope):
        return "two"

    async def allows(self, principal, action, resource):
        return resource == "*" or resource.partition("-")[0] == principal

    async def allows_creation(self, principal, resource, storage_profile):
        self.creations.append((principal, resource, storage_profile))
        return (
            storage_profile is not None
            and storage_profile.partition("-")[0] == principal
        )


def tenant_config(config, tmp_path):
    return replace(
        config,
        authorization=TenantGrants(),
        storage_profiles={
            name: Storage(root=tmp_path / name)
            for name in ("default", "one-log", "two-log", "two-sessions")
        },
    )


@pytest.mark.parametrize(
    ("query", "status"),
    [
        ("storage_profile=one-log", 403),
        ("", 403),
        ("storage_profile=two-log&storage_profile=one-log", 422),
        ("storage_profile=two%2Dlog", 422),
        ("%73torage_profile=two-log", 422),
        ("storage_profile=two-log&%73torage_profile=one-log", 422),
    ],
)
async def test_creation_authorizes_profile_before_registry(
    config, tmp_path, query, status
):
    settings = tenant_config(config, tmp_path)
    async with managed(settings) as (owner, client, _):
        response = await client.put(
            "/control/v1/streams/two-foreign?initialization=create_if_absent&" + query,
            headers={"If-None-Match": "*"},
            json=definition(name="two-foreign"),
        )
        assert response.status_code == status, response.text
        assert owner.registry.load() == ({}, 0)
        assert owner.records == {}
        assert owner.queue.empty()
        assert not any(
            storage.root.exists() for storage in settings.storage_profiles.values()
        )


@pytest.mark.parametrize("profile", ["two-log", "two-sessions"])
async def test_own_profiles_create_and_updates_keep_profile(
    config, tmp_path, profile, monkeypatch
):
    settings = tenant_config(config, tmp_path)
    async with managed(settings) as (owner, client, _):
        grant = settings.authorization.allows_creation

        async def checked_grant(principal, resource, storage_profile):
            assert owner.serial.locked()
            assert owner.registry.load() == ({}, 0)
            assert resource not in owner.records
            return await grant(principal, resource, storage_profile)

        monkeypatch.setattr(settings.authorization, "allows_creation", checked_grant)
        created = await put(
            owner, client, "two-events", definition(), storage_profile=profile
        )
        assert created.status_code == 202, created.text
        assert owner.records["two-events"]["storage_profile"] == profile
        assert settings.authorization.creations == [("two", "two-events", profile)]

        # Omitting a profile on an update retains the creation's selection.
        updated = await client.put(
            "/control/v1/streams/two-events",
            headers={"If-Match": '"1"'},
            json=definition(name="two-events", max_backlog=10),
        )
        assert updated.status_code == 202, updated.text
        await owner.queue.join()
        assert owner.records["two-events"]["storage_profile"] == profile
        assert settings.authorization.creations == [("two", "two-events", profile)]

        denied = await put(
            owner,
            client,
            "two-events",
            definition(max_backlog=10),
            storage_profile="one-log",
            revision=2,
        )
        assert denied.status_code == 409, denied.text
        assert denied.json()["error"]["code"] == "immutable_storage"
        assert owner.records["two-events"]["revision"] == 2


async def test_existing_authorization_retains_encoded_profile_support(config):
    async with managed(config) as (owner, client, _):
        response = await client.put(
            "/control/v1/streams/events?storage_profile=%6Cocal&initialization=create_if_absent",
            headers={"If-None-Match": "*"},
            json=definition(),
        )
        assert response.status_code == 202, response.text
        await owner.queue.join()
        assert owner.records["events"]["storage_profile"] == "local"
