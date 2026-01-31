from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import OAuth2PasswordRequestForm

from src.common.auth import create_access_token, verify_admin

from src.common.config import get_settings
from src.common.models import (
    GossipMessage,
    HealthCheckResponse,
    APITokenPayload,
    OPCUADataPoint,
    AntiEntropyRequest,
    AddOPCServerRequest
)
from src.common.auth import verify_token
from src.common.logger import get_logger
from src.common.utils import utc_now
from src.node.lamport import LamportClock
from src.node.storage import StorageManager
from src.node.gossip import GossipProtocol
from src.node.ingestor import OPCUAIngestor
from src.node.anti_entropy import AntiEntropyProtocol

import aiohttp
import asyncio
from src.common.models import ServerConfig


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
        
        self.app.post("/api/v1/auth/token")(self.login)

        
        self.app.get("/api/v1/health")(self.health_check)
        self.app.get("/api/v1/servers")(self.list_opcua_servers_name)
        self.app.get("/api/v1/tags")(self.list_all_tags)
        self.app.get("/api/v1/servers/{server_name}/tags")(self.list_tags)
        self.app.get("/api/v1/servers/{server_name}/tags/{tag}/latest")(self.get_tag_latest)
        self.app.get("/api/v1/servers/{server_name}/tags/{tag}/history")(self.get_tag_history)
        self.app.get("/api/v1/status")(self.get_node_status)
        

        # da docs l'utente non li deve vedere -> TODO aggiungere blocco con middleware o direttame
        self.app.post("/internal/gossip/ping", include_in_schema=False)(self.handle_gossip_ping)
        self.app.post("/internal/anti-entropy/sync", include_in_schema=False)(self.handle_anti_entropy_sync)
        self.app.post("/internal/anti-entropy/sync-servers", include_in_schema=False)(self.handle_server_config_sync)
        self.app.get("/internal/status", include_in_schema=False)(self.internal_status)

        self.app.get("/api/v1/admin/opc-servers")(self.list_opc_servers)
        self.app.post("/api/v1/admin/opc-servers")(self.add_opc_server)
        
    
    async def health_check(self) -> HealthCheckResponse:
        """
        endpoint di health check per verificare stato del nodo
        
        modificato per ascoltare solo in localhost direttamente nel docker compose
        """

        uptime = (utc_now() - self.start_time).total_seconds()
        current_lc = await self.lamport_clock.get_time()
        records_count = await self.storage.count_total_records()
        
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
    

    async def login(self, form_data: OAuth2PasswordRequestForm = Depends()) -> dict[str, Any]:
        """
        endpoint per generare token JWT via username/password
        """

        settings = get_settings()
        
        #check se è l'utente admin
        is_admin = (
            form_data.username == settings.admin_username and
            form_data.password == settings.admin_password.get_secret_value()
        )
        
        if is_admin:
            # token admin con tutti i permessi
            access_token = create_access_token(
                client_id=form_data.username,
                role="admin",
                scopes=["read", "write", "admin"]
            )
            
            return {
                "access_token": access_token,
                "token_type": "bearer",
                "role": "admin"
            }
        else:
            #token user con permessi limitati
            access_token = create_access_token(
                client_id=form_data.username,
                role="user",
                scopes=["read"]
            )
            
            return {
                "access_token": access_token,
                "token_type": "bearer",
                "role": "user"
            }
        
    
    async def list_tags(
        self,
        server_name: str,
        token: APITokenPayload = Depends(verify_token)
    ) -> list[str]:
        """
        elenca tutti i tag disponibili per uno specifico server
        """
        try:
            if not await self.storage.srv_exists(server_name=server_name):
                raise HTTPException(
                    status_code=404,
                    detail=f"server '{server_name}' non trovato"
                )
            
            all_tags = await self.storage.get_all_tags_from_srv(server_name=server_name)
            return all_tags
        
        except Exception as e:
            logger.error(f"errore listing tags: {e}")
            raise HTTPException(status_code=500, detail=str(e))
        
    
    async def list_all_tags(
        self,
        token: APITokenPayload = Depends(verify_token)
    ) -> list[dict[str, str]]:
        """
        elenca tutti i tag disponibili per tutti i server
        """
        try:
        
            all_tags = await self.storage.get_all_tags()
            return all_tags
        
        except Exception as e:
            logger.error(f"errore listing tags: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    async def get_tag_latest(
        self,
        server_name: str,
        tag: str,
        token: APITokenPayload = Depends(verify_token)
    ) -> dict[str, Any]:
        """
        ottiene l'ultimo valore di un tag specifico per un dato server
        
        args:
            tag: nome del tag da cercare
        """
        try:
            if not await self.storage.srv_exists(server_name=server_name):
                raise HTTPException(
                    status_code=404,
                    detail=f"server '{server_name}' non trovato"
                )

            data_point: OPCUADataPoint = await self.storage.get_latest_tag(tag=tag, server_name=server_name)
            
            if not data_point:
                raise HTTPException(
                    status_code=404,
                    detail=f"tag '{tag}' non trovato per il server '{server_name}'"
                )
            
            return {
                "tag": data_point.tag,
                "value": data_point.value,
                "timestamp": data_point.timestamp.isoformat(),
                "quality": data_point.quality.value,
                "lamport_clock": data_point.lamport_clock,
                "source_server": data_point.source_server,
                "node_id": self.node_id,
                "server_name": data_point.server_name
            }
            
        except HTTPException:
            raise

        except Exception as e:
            logger.error(f"errore get_tag_latest per {tag}: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    
    async def get_tag_history(
        self,
        server_name: str,
        tag: str,
        start: datetime | None = Query(None, description="timestamp iniziale"),
        end: datetime | None = Query(None, description="timestamp finale"),
        limit: int = Query(1000, ge=1, le=10000, description="max record da restituire"),
        token: APITokenPayload = Depends(verify_token)
    ) -> dict[str, Any]:
        """
        ottiene lo storico di un tag in un range temporale
        
        args:
            server_name: server sorgente,
            tag: nome del tag
            start: timestamp iniziale
            end: timestamp finale
            limit: massimo numero di record

        start timestamp e end sono opzionali
        """
        try:
            if not await self.storage.srv_exists(server_name=server_name):
                raise HTTPException(
                    status_code=404,
                    detail=f"server '{server_name}' non trovato"
                )
            
            # verifica che il tag esista
            all_tags = await self.storage.get_all_tags_from_srv(server_name=server_name)
            if tag not in all_tags:
                raise HTTPException(
                    status_code=404,
                    detail=f"tag '{tag}' non trovato per il server '{server_name}'"
                )
            
            history = await self.storage.get_tag_history(
                tag=tag,
                server_name=server_name,
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
        ottiene lo status completo del nodo
        """
        try:
            current_lc = await self.lamport_clock.get_time()
            uptime = (utc_now() - self.start_time).total_seconds()
            
            ingestor_status = self.ingestor.get_status()
            gossip_status = self.gossip.get_peer_status()
            anti_entropy_stats = self.anti_entropy.get_stats()
            
            storage_count = await self.storage.count_total_records()
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
    

    def list_opcua_servers_name(self, token: APITokenPayload = Depends(verify_token)) -> list[str]:
        """get di tutti i nomi dei server opcua a cui il nodo fa polling"""

        return self.ingestor.get_opcua_servers_names()
    

    async def list_opc_servers(
        self,
        token: APITokenPayload = Depends(verify_admin)
    ) -> dict[str, Any]:
        """get di tutti i server opcua a cui il nodo fa polling"""

        servers = self.ingestor.get_opcua_servers()
        return {
            "servers": servers,
            "count": len(servers)
        }


    async def add_opc_server(
    self,
    request: AddOPCServerRequest,
    token: APITokenPayload = Depends(verify_admin)
) -> dict[str, Any]:
        """
        aggiunge un nuovo server OPC UA a runtime e lo salva nel database
        la replicazione avviene automaticamente tramite anti-entropy
        """
        
        # incrementa lamport clock per questo evento
        lc = await self.lamport_clock.tick()
        
        # crea configurazione server
        server_config = ServerConfig(
            server_name=request.server_name,
            endpoint=request.endpoint,
            lamport_clock=lc,
            node_id=self.node_id
        )
        
        #save nel database locale
        insert = await self.storage.insert_server_config(server_config)
        if not insert:
            raise HTTPException(
                status_code=500,
                detail=f"errore dell'applicazione nell'aggiunta del server '{request.server_name}'"
            )

        # aggiungo all'ingestor
        success = await self.ingestor.add_server(
            endpoint=request.endpoint,
            server_name=request.server_name
        )
        
        if not success:
            raise HTTPException(
                status_code=400,
                detail=f"impossibile aggiungere server '{request.server_name}'. Verifica che l'endpoint sia raggiungibile e che il nome non sia già in uso."
            )
        
        logger.info(f"server 0{request.server_name}' aggiunto con LC={lc}, sarà replicato via anti-entropy")
        
        return {
            "status": "success",
            "message": f"server '{request.server_name}' aggiunto e sarà replicato automaticamente",
            "endpoint": request.endpoint,
            "server_name": request.server_name,
            "lamport_clock": lc
        }
    

    async def handle_server_config_sync(self, request: dict[str, Any]) -> dict[str, Any]:
        """
        gestione richiesta di sincronizzazione configurazioni server da altri nodi
        
        args:
            request - richiesta di sync delle config server
        """
        try:
            from src.common.models import ServerConfigSyncRequest
            
            sync_request = ServerConfigSyncRequest(**request)
            response = await self.anti_entropy.handle_server_config_sync_request(sync_request)
            
            return response
            
        except Exception as e:
            logger.error(f"errore gestione server config sync da {request.get('requester_id')}: {e}")
            raise HTTPException(status_code=500, detail=str(e))


    def get_app(self) -> FastAPI:
        # funzione per farsi restituire tutti l'app fastapi. serve per poi runnnare l'applicazione con uvicorn
        return self.app