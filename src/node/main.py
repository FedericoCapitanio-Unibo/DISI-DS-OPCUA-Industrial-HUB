"""
entry point principale del nodo HUB.
inizializza e coordina tutti i componenti: ingestor, storage, gossip, API.
"""

import asyncio
import signal
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from src.common.config import get_settings
from src.common.logger import setup_logger
from src.node.lamport import LamportClock
from src.node.storage import StorageManager
from src.node.ingestor import OPCUAIngestor
from src.node.gossip import GossipProtocol
from src.node.anti_entropy import AntiEntropyProtocol
from src.node.api import HubAPI

logger = setup_logger(__name__, level="INFO", json_format=False)


class HubNode:
    """nodo HUB completo che contiene e gesticei gli altr componenti"""
    
    def __init__(self) -> None:
        settings = get_settings()
        
        self.node_id = settings.node_id
        self.port = settings.node_port
        
        self.lamport_clock = LamportClock()
        self.storage: StorageManager | None = None
        self.ingestor: OPCUAIngestor | None = None
        self.gossip: GossipProtocol | None = None
        self.anti_entropy: AntiEntropyProtocol | None = None
        self.api: HubAPI | None = None
        
        logger.info(f"nodo {self.node_id} inizializzato")
    
    
    async def initialize(self) -> None:
        """inizializza tutti i componenti in sequenza"""
        
        logger.info("inizializzazione componenti...")
        
        #storage
        self.storage = StorageManager()
        await self.storage.initialize()
        logger.info("storage pronto")
        
        # ingestor opcua
        self.ingestor = OPCUAIngestor(
            lamport_clock=self.lamport_clock,
            storage=self.storage
        )
        await self.ingestor.start()
        logger.info(f"ingestor avviato con {len(self.ingestor.connections)} canali OPCUA")
        
        #gossip protocol
        self.gossip = GossipProtocol(
            node_id=self.node_id,
            node_port=self.port,
            lamport_clock=self.lamport_clock
        )
        await self.gossip.start()
        logger.info("gossip avviato")
        
        #anti-entropy protocol
        self.anti_entropy = AntiEntropyProtocol(
            node_id=self.node_id,
            lamport_clock=self.lamport_clock,
            storage=self.storage,
            gossip=self.gossip
        )
        await self.anti_entropy.start()
        logger.info("anti-entropy avviato")
        
        # interfaccia con le API
        self.api = HubAPI(
            lamport_clock=self.lamport_clock,
            storage=self.storage,
            gossip=self.gossip,
            ingestor=self.ingestor,
            anti_entropy=self.anti_entropy
        )
        logger.info("API pronta")
        
        logger.info(f"nodo {self.node_id} completamente operativo")
    
    
    async def shutdown(self) -> None:

        logger.info("shutdown nodo in corso...")
        
        if self.ingestor:
            await self.ingestor.stop()
            logger.info("ingestor fermato")
        
        if self.gossip:
            await self.gossip.stop()
            logger.info("gossip fermato")
        
        if self.anti_entropy:
            await self.anti_entropy.stop()
            logger.info("anti-entropy fermato")
        
        if self.storage:
            await self.storage.close()
            logger.info("storage chiuso")
        
        logger.info(f"nodo {self.node_id} terminato")
    
    
    def get_app(self) -> FastAPI:

        if not self.api:
            raise RuntimeError("API non inizializzata, chiama initialize() prima")
        
        return self.api.get_app()


# istanza globale del nodo
_node: HubNode | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):

    global _node    
    _node = HubNode()
    await _node.initialize()
    
    yield
    
    if _node:
        await _node.shutdown()


def create_app() -> FastAPI:

    # crea nodo temporaneo per ottenere l'app. il vero nodo sarà inizializzato nel lifespan
    node = HubNode()
    node.lamport_clock = LamportClock()
    node.storage = StorageManager()
    node.ingestor = OPCUAIngestor(node.lamport_clock, node.storage)
    node.gossip = GossipProtocol(node.node_id, node.port, node.lamport_clock)
    node.anti_entropy = AntiEntropyProtocol(node.node_id, node.lamport_clock, node.storage, node.gossip)
    node.api = HubAPI(node.lamport_clock, node.storage, node.gossip, node.ingestor, node.anti_entropy)
    
    app = node.get_app()
    
    # sostituisci lifespan
    app.router.lifespan_context = lifespan
    
    return app


async def main() -> None:

    # get delle config
    settings = get_settings()
    
    logger.info("=" * 60)
    logger.info(f"avvio nodo HUB: {settings.node_id}")
    logger.info(f"porta: {settings.node_port}")
    logger.info("=" * 60)
    
    # creazione nodo
    node = HubNode()
    await node.initialize()
    
    # gestione segnali per shutdown controllaro
    def signal_handler(sig, frame):
        logger.info(f"ricevuto segnale {sig}, shutdown in corso...")
        asyncio.create_task(node.shutdown())
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    #avvio server uvicorn
    config = uvicorn.Config(
        app=node.get_app(),
        host="0.0.0.0",
        port=settings.node_port,
        log_level="info",
        timeout_graceful_shutdown=10
    )
    
    server = uvicorn.Server(config=config)
    
    try:
        await server.serve()
    except KeyboardInterrupt:
        logger.info("interruzione keyboard ricevuta")
    finally:
        await node.shutdown()


if __name__ == "__main__":
    asyncio.run(main())