"""Knowledge Assistant helpers via ``WorkspaceClient.knowledge_assistants`` (SDK API)."""

from databricks.sdk.service.knowledgeassistants import FilesSpec, KnowledgeAssistant, KnowledgeSource


def list_knowledge_assistants(
    workspace_client,
    *,
    page_size: int | None = None,
    page_token: str | None = None,
):
    """List Knowledge Assistants using the SDK iterator.

    ``page_size`` / ``page_token`` are passed to
    ``knowledge_assistants.list_knowledge_assistants`` (see SDK docs). The iterator may
    follow multiple pages; this function materializes all yielded assistants into one list.

    Returns ``{"knowledge_assistants": [<dict>, ...], "next_page_token": None}`` for
    notebook-style ``.get()`` access. ``next_page_token`` is always ``None`` because the
    high-level SDK exposes an iterator rather than a single-page response wrapper.
    """
    ka = workspace_client.knowledge_assistants
    assistants = [a.as_dict() for a in ka.list_knowledge_assistants(page_size=page_size, page_token=page_token)]
    return {"knowledge_assistants": assistants, "next_page_token": None}


def get_knowledge_assistant_id_by_name(workspace_client, name: str) -> str | None:
    """Return the ``id`` of the first Knowledge Assistant whose ``name`` equals ``name`` (stripped), else ``None``.

    Walks the SDK list iterator until a match or exhaustion.
    """
    want = name.strip()
    for assistant in workspace_client.knowledge_assistants.list_knowledge_assistants(page_size=100):
        if (assistant.name or "").strip() == want:
            return assistant.id
    return None


def get_knowledge_assistant_id_by_display_name(workspace_client, display_name: str) -> str | None:
    """Return the ``id`` of the first Knowledge Assistant whose ``display_name`` matches (stripped), else ``None``.

    Walks the SDK list iterator until a match or exhaustion.
    """
    want = display_name.strip()
    for assistant in workspace_client.knowledge_assistants.list_knowledge_assistants(page_size=100):
        if (assistant.display_name or "").strip() == want:
            return assistant.id
    return None


def create_knowledge_assistant(
    workspace_client,
    display_name: str,
    description: str,
    instructions: str | None = None,
):
    """Create a Knowledge Assistant. Returns the create response as a dict (id, name, state, etc.)."""
    body = KnowledgeAssistant(
        display_name=display_name,
        description=description,
        instructions=instructions,
    )
    created = workspace_client.knowledge_assistants.create_knowledge_assistant(knowledge_assistant=body)
    return created.as_dict()


def get_knowledge_assistant(workspace_client, id: str):
    """Get a Knowledge Assistant by id. Returns the full response as a dict or raises."""
    name = f"knowledge-assistants/{id}"
    got = workspace_client.knowledge_assistants.get_knowledge_assistant(name=name)
    return got.as_dict()


def create_knowledge_source_files(
    workspace_client,
    assistant_id: str,
    display_name: str,
    description: str,
    volume_path: str,
):
    """Create a 'files' Knowledge Source pointing at a UC volume path. Returns the create response as a dict."""
    parent = f"knowledge-assistants/{assistant_id}"
    source = KnowledgeSource(
        display_name=display_name,
        description=description,
        source_type="files",
        files=FilesSpec(path=volume_path),
    )
    created = workspace_client.knowledge_assistants.create_knowledge_source(parent=parent, knowledge_source=source)
    return created.as_dict()


def get_knowledge_source(workspace_client, assistant_id: str, source_id: str):
    """Get a Knowledge Source by assistant id and source id. Returns the full response as a dict or raises."""
    name = f"knowledge-assistants/{assistant_id}/knowledge-sources/{source_id}"
    got = workspace_client.knowledge_assistants.get_knowledge_source(name=name)
    return got.as_dict()


def sync_knowledge_sources(workspace_client, assistant_id: str):
    """Sync all non-index Knowledge Sources for a Knowledge Assistant. Returns ``{}`` (no response body from API)."""
    workspace_client.knowledge_assistants.sync_knowledge_sources(name=f"knowledge-assistants/{assistant_id}")
    return {}
