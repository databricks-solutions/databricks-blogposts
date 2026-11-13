"""The memory actor is derived on the server, never taken from the request body."""

import json

import pytest
from runtime import identity


def test_actor_is_signed_in_user():
    assert identity.signed_in_user({b"x-forwarded-email": b"maria@corp.com"}) == "maria@corp.com"
    assert identity.actor_for("maria@corp.com") == "maria@corp.com"


def test_personas_are_namespaced_under_the_signed_in_user(monkeypatch):
    monkeypatch.setenv("MAINTBOT_DEMO_PERSONAS", "true")
    assert identity.actor_for("tech@corp.com", "maria") == "tech@corp.com/maria"
    # A persona's team memory is its plant's, shared with everyone at that plant.
    assert identity.team_actor(identity.plant_for("tech@corp.com", "maria")) == "plant-p2"
    with pytest.raises(ValueError):
        identity.actor_for("tech@corp.com", "someone-else")


def test_personas_ignored_when_disabled(monkeypatch):
    monkeypatch.delenv("MAINTBOT_DEMO_PERSONAS", raising=False)
    assert identity.actor_for("tech@corp.com", "maria") == "tech@corp.com"


def test_plant_comes_from_server_config(monkeypatch):
    monkeypatch.delenv("MAINTBOT_DEMO_PERSONAS", raising=False)
    monkeypatch.setenv("MAINTBOT_USER_PLANTS", '{"rosa@corp.com": "P4"}')
    monkeypatch.setenv("MAINTBOT_DEFAULT_PLANT", "P1")
    assert identity.plant_for("rosa@corp.com") == "P4"
    assert identity.plant_for("someone@corp.com") == "P1"


@pytest.mark.asyncio
async def test_middleware_overwrites_client_supplied_actor(monkeypatch):
    monkeypatch.setenv("MAINTBOT_DEMO_PERSONAS", "true")
    seen = {}

    async def app(scope, receive, send):
        seen["body"] = json.loads((await receive())["body"])

    body = json.dumps({"input": {"messages": [], "actor": "attacker", "persona": "jamal"}}).encode()
    messages = iter([{"type": "http.request", "body": body, "more_body": False}])

    async def receive():
        return next(messages)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/invocations",
        "headers": [(b"x-forwarded-email", b"jamal@corp.com")],
    }
    await identity.TrustedActorMiddleware(app)(scope, receive, None)
    assert seen["body"]["input"] == {"messages": [], "actor": "jamal@corp.com/jamal", "team": "plant-p1"}
