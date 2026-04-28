"""
Genie space management: create, get existing, and helpers.
API: https://docs.databricks.com/api/workspace/genie/createspace
"""


def create_genie_space(
    workspace_client,
    warehouse_id: str,
    serialized_space: str,
    title: str,
    description: str,
):
    """Create a Genie space via POST /api/2.0/genie/spaces. Returns the response (space_id, title, etc.)."""
    return workspace_client.api_client.do(
        "POST",
        "/api/2.0/genie/spaces",
        body={
            "warehouse_id": warehouse_id,
            "serialized_space": serialized_space,
            "title": title,
            "description": description,
        },
    )


def get_existing_genie_space(workspace_client, title: str):
    """Return the space dict (with space_id, title, ...) if a space with this title exists, else None."""
    page_token = None
    while True:
        payload = {"page_size": 100}
        if page_token:
            payload["page_token"] = page_token
        response = workspace_client.api_client.do(
            "GET", "/api/2.0/genie/spaces", query=payload
        )
        spaces = response.get("spaces") or []
        for s in spaces:
            if (s.get("title") or "").strip() == (title or "").strip():
                return s
        page_token = response.get("next_page_token")
        if not page_token:
            break
    return None


def print_space_info(space: dict, *, created: bool = False) -> None:
    """Print space_id, title, description, and warehouse_id."""
    space_id = space.get("space_id")
    title = space.get("title", "")
    description = space.get("description", "")
    warehouse_id = space.get("warehouse_id", "")
    if created:
        print("Created Genie space.")
    else:
        print("Genie space already exists.")
    print(f"space_id: {space_id}")
    print(f"title: {title}")
    print(f"description: {description}")
    print(f"warehouse_id: {warehouse_id}")
