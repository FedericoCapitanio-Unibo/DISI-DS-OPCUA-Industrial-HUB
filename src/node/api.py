"""
api REST del nodo HUB usando FastAPI.
espone endpoint pubblici per query dati e interni per gossip/anti-entropy.
"""

from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.responses import JSONResponse

from src.common.config import get_settings
from src.common.models import (
    GossipMessage,
    HealthCheckResponse,
    APITokenPayload,
    OPCUADataPoint,
    AntiEntropyRequest
)
from src.common.auth import verify_token
from src.common.logger import get_logger
from src.common.utils import utc_now
from src.node.lamport import LamportClock
from src.node.storage import StorageManager
from src.node.gossip import GossipProtocol
from src.node.ingestor import OPCUAIngestor
from src.node.anti_entropy import AntiEntropyProtocol

logger = get_logger(__name__)


class HubAPI:
    """
    gestisce tutte le api del nodo hub
    """
    
    def __init__(
        self,
        lamport_clock: LamportClock,
        storage: StorageManager,
        gossip: GossipProtocol,
        ingestor: OPCUAIngestor,
        anti_entropy: AntiEntropyProtocol
    ) -> None:
        """
        lamport_clock: clock logico del nodo
        storage: storage manager per query dati
        gossip: protocollo gossip per failure detection
        ingestor: ingestor OPC UA per statistiche
        anti_entropy: protocollo anti-entropy per sincronizzazione
        """
        self.lamport_clock = lamport_clock
        self.storage = storage
        self.gossip = gossip
        self.ingestor = ingestor
        self.anti_entropy = anti_entropy
        
        # configurazione
        settings = get_settings()
        self.node_id = settings.node_id
        
        # crea app FastAPI
        self.app = FastAPI(
            title="OPC UA Industrial HUB",
            description="nodo per raccolta dati OPC UA",
            version="0.1.0",
        )
        
        # registra rotte api
        self._register_routes()
        
        self.start_time = utc_now()
        
        logger.info("API inizializzata")
    
    
    def _register_routes(self) -> None:
        """registra tutti gli endpoint dell'API"""
        
        # endpoint pubblici (con auth token)
        self.app.get("/api/v1/health")(self.health_check)
        self.app.get("/api/v1/tags")(self.list_tags)
        self.app.get("/api/v1/tags/{tag}/latest")(self.get_tag_latest)
        self.app.get("/api/v1/tags/{tag}/history")(self.get_tag_history)
        self.app.get("/api/v1/status")(self.get_node_status)
        
        # endpoint interni (senza auth dato che sono per comunicazione tra nodi)
        self.app.post("/internal/gossip/ping")(self.handle_gossip_ping)
        self.app.post("/internal/anti-entropy/sync")(self.handle_anti_entropy_sync)
        self.app.get("/internal/status")(self.internal_status)
    
    
    async def health_check(self) -> HealthCheckResponse:
        """
        endpoint di health check per verificare stato del nodo
        
        !!! è pubblico ma senza autenticazione per monitoring esterno
        """

        uptime = (utc_now() - self.start_time).total_seconds()
        current_lc = await self.lamport_clock.get_time()
        records_count = await self.storage.count_records()
        
        ingestor_status = self.ingestor.get_status()
        gossip_status = self.gossip.get_peer_status()
        
        return HealthCheckResponse(
            status="healthy",
            node_id=self.node_id,
            lamport_clock=current_lc,
            uptime_seconds=uptime,
            connected_opc_servers=ingestor_status["connected_servers"],
            active_peers=gossip_status["alive_peers"],
            storage_records_count=records_count
        )
    
    
    async def list_tags(
        self,
        token: APITokenPayload = Depends(verify_token)
    ) -> dict[str, Any]:
        """
        elenca tutti i tag disponibili nel sistema
        """
        try:
            tags = await self.storage.get_all_tags()
            
            return {
                "tags": tags,
                "count": len(tags),
                "node_id": self.node_id
            }
            
        except Exception as e:
            logger.error(f"errore listing tags: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    async def get_tag_latest(
        self,
        tag: str,
        token: APITokenPayload = Depends(verify_token)
    ) -> dict[str, Any]:
        """
        ottiene l'ultimo valore di un tag specifico
        
        args:
            tag: nome del tag da cercare
        """
        try:
            data_point = await self.storage.get_latest_by_tag(tag)
            
            if not data_point:
                raise HTTPException(
                    status_code=404,
                    detail=f"tag '{tag}' non trovato"
                )
            
            return {
                "tag": data_point.tag,
                "value": data_point.value,
                "timestamp": data_point.timestamp.isoformat(),
                "quality": data_point.quality.value,
                "lamport_clock": data_point.lamport_clock,
                "source_server": data_point.source_server,
                "node_id": self.node_id
            }
            
        except HTTPException:
            raise

        except Exception as e:
            logger.error(f"errore get_tag_latest per {tag}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    async def get_tag_history(
        self,
        tag: str,
        start: datetime | None = Query(None, description="timestamp iniziale"),
        end: datetime | None = Query(None, description="timestamp finale"),
        limit: int = Query(1000, ge=1, le=10000, description="max record da restituire"),
        token: APITokenPayload = Depends(verify_token)
    ) -> dict[str, Any]:
        """
        ottiene lo storico di un tag in un range temporale
        
        args:
            tag: nome del tag
            start: timestamp iniziale
            end: timestamp finale
            limit: massimo numero di record

        start timestamp e end sono opzionali
        """
        try:
            # verifica che il tag esista
            all_tags = await self.storage.get_all_tags()
            if tag not in all_tags:
                raise HTTPException(
                    status_code=404,
                    detail=f"tag '{tag}' non trovato"
                )
            
            history = await self.storage.get_history_by_tag(
                tag=tag,
                start_time=start,
                end_time=end,
                limit=limit
            )
            
            return {
                "tag": tag,
                "data": [
                    {
                        "value": dp.value,
                        "timestamp": dp.timestamp.isoformat(),
                        "quality": dp.quality.value,
                        "lamport_clock": dp.lamport_clock
                    }
                    for dp in history
                ],
                "count": len(history),
                "node_id": self.node_id
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"errore get_tag_history per {tag}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    async def get_node_status(
        self,
        token: APITokenPayload = Depends(verify_token)
    ) -> dict[str, Any]:
        """
        ottiene lo status completo del nodo (ingestor + gossip + storage).
        richiede autenticazione JWT.
        """
        try:
            current_lc = await self.lamport_clock.get_time()
            uptime = (utc_now() - self.start_time).total_seconds()
            
            ingestor_status = self.ingestor.get_status()
            gossip_status = self.gossip.get_peer_status()
            anti_entropy_stats = self.anti_entropy.get_stats()
            
            storage_count = await self.storage.count_records()
            storage_max_lc = await self.storage.get_max_lamport_clock()
            storage_min_lc = await self.storage.get_min_lamport_clock()
            
            return {
                "node_id": self.node_id,
                "lamport_clock": current_lc,
                "uptime_seconds": uptime,
                "ingestor": ingestor_status,
                "gossip": gossip_status,
                "anti_entropy": anti_entropy_stats,
                "storage": {
                    "records_count": storage_count,
                    "min_lamport_clock": storage_min_lc,
                    "max_lamport_clock": storage_max_lc
                }
            }
            

        except Exception as e:
            logger.error(f"errore get_node_status: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    async def handle_gossip_ping(self, ping: GossipMessage) -> dict[str, Any]:
        """
        gestisce ping ricevuti da altri nodi (endpoint interno)

        args:
            ping: messaggio gossip ricevuto"""
        try:
            ack = await self.gossip.handle_incoming_ping(ping)
            
            return ack
            
        except Exception as e:
            logger.error(f"errore gestione ping da {ping.sender_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    async def handle_anti_entropy_sync(self, request: AntiEntropyRequest) -> dict[str, Any]:
        """
        gestisce richiesta di sincronizzazione anti-entropy da altri nodi        
        args:
            request: richiesta anti-entropy
        """

        try:
            response = await self.anti_entropy.handle_sync_request(request)
            return response
            
        except Exception as e:
            logger.error(f"errore gestione anti-entropy da {request.requester_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    async def internal_status(self) -> dict[str, Any]:
        """
        status semplificato per comunicazione interna tra nodi
        """
        try:
            current_lc = await self.lamport_clock.get_time()
            storage_max_lc = await self.storage.get_max_lamport_clock()
            
            return {
                "node_id": self.node_id,
                "lamport_clock": current_lc,
                "storage_max_lc": storage_max_lc,
                "timestamp": utc_now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"errore internal_status: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    def get_app(self) -> FastAPI:
        # funzione per farsi restituire tutti l'app fastapi. serve per poi runnnare l'applicazione con uvicorn
        return self.app