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

        #tiro su tutti i componenti

        self.storage = StorageManager()
        await self.storage.initialize()
        logger.info("storage pronto")
        
        # salvo i server iniziali da config nel database se non ci sono già
        server_configs = await self.storage.get_all_server_configs()
        logger.info(f"server già nel database: {len(server_configs)}")
        
        if not server_configs:
            # nessun server nel database, salva quelli da env con LC FISSI
            settings = get_settings()
            initial_servers = settings.get_opc_servers_list()
            
            if initial_servers:
                logger.info(f"inizializzazione database con {len(initial_servers)} server da configurazione")
                
                from src.common.models import ServerConfig
                
                #utilizzo di lamport fissi (1, 2, 3...) e node_id="system" per garantire consistenza
                for i, endpoint in enumerate(initial_servers):
                    server_name = f"OPCServer{i+1}"
                    lc = i + 1
                    
                    config = ServerConfig(
                        server_name=server_name,
                        endpoint=endpoint,
                        lamport_clock=lc,
                        node_id="system"  # "system" invece del node_id (self.node_id) specifico
                    )
                    
                    if await self.storage.insert_server_config(config):
                        logger.info(f"salvato '{server_name}'({endpoint}) con LC={lc}")
                    else:
                        logger.error(f"'{server_name}'({endpoint}) non aggiunto correttamente")            


                #aggiornp lamport clock per partire dopo i server iniziali
                await self.lamport_clock.update(len(initial_servers))
        else:
            # se ci sono già dei serveraggiorno il lamport clock
            max_lc = max(c.lamport_clock for c in server_configs)
            await self.lamport_clock.update(max_lc)
            logger.info(f"lamport clock aggiornato a {max_lc}")
        
        self.ingestor = OPCUAIngestor(
            lamport_clock=self.lamport_clock,
            storage=self.storage
        )
        await self.ingestor.start()
        logger.info(f"ingestor avviato con {len(self.ingestor.connections)} canali OPCUA")
        
        self.gossip = GossipProtocol(
            node_id=self.node_id,
            node_port=self.port,
            lamport_clock=self.lamport_clock
        )
        await self.gossip.start()
        logger.info("gossip avviato")
        
        self.anti_entropy = AntiEntropyProtocol(
            node_id=self.node_id,
            lamport_clock=self.lamport_clock,
            storage=self.storage,
            gossip=self.gossip,
            ingestor=self.ingestor
        )
        await self.anti_entropy.start()
        logger.info("anti-entropy avviato")
        
        self.api = HubAPI(
            lamport_clock=self.lamport_clock,
            storage=self.storage,
            gossip=self.gossip,
            ingestor=self.ingestor,
            anti_entropy=self.anti_entropy
        )
        logger.info("API pronta")
        
        logger.info(f"nodo '{self.node_id}' completamente operativo")
    
    
    async def shutdown(self) -> None:

        logger.info("shutdown nodo in corso...")
        

        try:
            if self.ingestor:
                await self.ingestor.stop()
                logger.info("ingestor fermato")
        except asyncio.CancelledError:
            pass

        try:
            if self.gossip:
                await self.gossip.stop()
                logger.info("gossip fermato")
        except asyncio.CancelledError:
            pass

        try:
            if self.anti_entropy:
                await self.anti_entropy.stop()
                logger.info("anti-entropy fermato")
        except asyncio.CancelledError:
            pass

        try:
            if self.storage:
                await self.storage.close()
                logger.info("storage chiuso")
        except asyncio.CancelledError:
            pass


        logger.info(f"nodo '{self.node_id}' terminato")
    
    
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

    #creazione nodo temporaneo per ottenere l'app perhcè il vero nodo sarà inizializzato nel lifespan
    
    node = HubNode()
    node.lamport_clock = LamportClock()
    node.storage = StorageManager()
    node.ingestor = OPCUAIngestor(node.lamport_clock, node.storage)
    node.gossip = GossipProtocol(node.node_id, node.port, node.lamport_clock)
    node.anti_entropy = AntiEntropyProtocol(node.node_id, node.lamport_clock, node.storage, node.gossip, node.ingestor)
    node.api = HubAPI(node.lamport_clock, node.storage, node.gossip, node.ingestor, node.anti_entropy)
    
    app = node.get_app()
    
    # sostituisci lifespan
    app.router.lifespan_context = lifespan
    
    return app


async def main() -> None:

    # get delle config
    settings = get_settings()
    
    logger.info("=" * 60)
    logger.info(f"avvio nodo HUB: '{settings.node_id}'")
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