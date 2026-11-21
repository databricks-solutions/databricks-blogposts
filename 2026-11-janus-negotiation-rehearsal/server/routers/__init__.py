from server.routers.health import router as health_router
from server.routers.matters import router as matters_router
from server.routers.documents import router as documents_router
from server.routers.extraction import router as extraction_router
from server.routers.simulation import router as simulation_router
from server.routers.chat import router as chat_router
from server.routers.output import router as output_router
from server.routers.governance import router as governance_router

all_routers = [
    health_router,
    matters_router,
    documents_router,
    extraction_router,
    simulation_router,
    chat_router,
    output_router,
    governance_router,
]
