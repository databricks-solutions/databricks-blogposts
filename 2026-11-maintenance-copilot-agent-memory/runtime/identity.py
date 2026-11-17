"""Who is asking: the trusted memory actors for a request.

MaintBot keeps two memories, both in one Agent Memory store and told apart by actor:

* **personal** — actor is the signed-in user. Preferences and formats that belong to one person.
* **team** — actor is the user's plant (``plant-p1``). Equipment quirks, fixes, data definitions,
  vendors, and contacts that everyone at the plant should benefit from.

Both actors must come from the authenticated identity, never from the model or the request body.
Databricks Apps forward the signed-in user in ``X-Forwarded-Email``; locally we fall back to the
workspace user behind your CLI profile. A user's plant comes from ``MAINTBOT_USER_PLANTS`` (a JSON
map of email to plant code) or ``MAINTBOT_DEFAULT_PLANT``.

For demos, ``MAINTBOT_DEMO_PERSONAS=true`` lets one signed-in user switch between technicians. A
persona's personal actor is namespaced under the real user (``<email>/<persona>``), so it can only
reach memory that belongs to the person signed in; its team actor is its plant's, shared like any
other user's.
"""

import json
import os
from functools import lru_cache

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from databricks_agentkit import workspace_client

PERSONAS = {
    "jamal": {"name": "Jamal Okafor", "role": "Electrical tech · nights", "plant": "P1"},
    "rosa": {"name": "Rosa Lindqvist", "role": "Mechanical tech · days", "plant": "P1"},
    "maria": {"name": "Maria Chen", "role": "Hydraulics lead · nights", "plant": "P2"},
}
PLANT_NAMES = {"P1": "Spokane", "P2": "Tacoma", "P3": "Boise", "P4": "Yakima"}

_USER_HEADERS = (b"x-forwarded-email", b"x-forwarded-user")


def personas_enabled() -> bool:
    return os.getenv("MAINTBOT_DEMO_PERSONAS", "").lower() in ("1", "true", "yes")


@lru_cache(maxsize=1)
def _local_user() -> str:
    try:
        return workspace_client().current_user.me().user_name or "local-dev"
    except Exception:
        return "local-dev"


def signed_in_user(headers: dict[bytes, bytes]) -> str:
    for name in _USER_HEADERS:
        if value := headers.get(name):
            return value.decode()
    return _local_user()


def _check_persona(persona: str | None) -> str | None:
    if not persona or not personas_enabled():
        return None
    if persona not in PERSONAS:
        raise ValueError(f"unknown persona {persona!r}")
    return persona


def actor_for(user: str, persona: str | None = None) -> str:
    """The personal memory actor for ``user``, optionally one of their demo personas."""
    persona = _check_persona(persona)
    return f"{user}/{persona}" if persona else user


def plant_for(user: str, persona: str | None = None) -> str:
    """The plant code whose team memory ``user`` (or their persona) shares."""
    if persona := _check_persona(persona):
        return PERSONAS[persona]["plant"]
    plants = json.loads(os.getenv("MAINTBOT_USER_PLANTS", "{}"))
    return plants.get(user, os.getenv("MAINTBOT_DEFAULT_PLANT", "P1"))


def team_actor(plant: str) -> str:
    return f"plant-{plant.lower()}"


class TrustedActorMiddleware:
    """Set ``input.actor`` and ``input.team`` on every invocation from the signed-in user.

    The adapter reads both from the request body; this makes them server-controlled. The body is
    rewritten before the runtime persists it, so background runs and recovery replay the trusted
    actors too. Clients may send ``input.persona`` (a key of PERSONAS).
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] != "POST" or scope["path"] != "/api/invocations":
            await self.app(scope, receive, send)
            return
        chunks = []
        while True:
            message = await receive()
            chunks.append(message.get("body", b""))
            if not message.get("more_body"):
                break
        body = b"".join(chunks)
        try:
            payload = json.loads(body or b"{}")
            agent_input = payload.get("input")
            if isinstance(agent_input, dict):
                user = signed_in_user(dict(scope["headers"]))
                persona = agent_input.pop("persona", None)
                agent_input["actor"] = actor_for(user, persona)
                agent_input["team"] = team_actor(plant_for(user, persona))
                body = json.dumps(payload).encode()
        except ValueError as error:
            await _reject(send, 400, str(error))
            return
        headers = [(k, v) for k, v in scope["headers"] if k != b"content-length"]
        scope = {**scope, "headers": [*headers, (b"content-length", str(len(body)).encode())]}
        sent = False

        async def replay() -> Message:
            nonlocal sent
            if sent:
                return await receive()
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}

        await self.app(scope, replay, send)


async def _reject(send: Send, status: int, detail: str) -> None:
    body = json.dumps({"detail": detail}).encode()
    await send({"type": "http.response.start", "status": status, "headers": [(b"content-type", b"application/json")]})
    await send({"type": "http.response.body", "body": body})
