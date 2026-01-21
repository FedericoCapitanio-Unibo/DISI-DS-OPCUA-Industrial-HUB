"""
protocollo anti-entropy perla sincronizzazione dei dati tra nodi. l'idea è che ogni nodo confronta periodicamente i propri dati con gli altri peer e recupera quelli mancanti

"""

import asyncio
from typing import Any

import aiohttp

from src.common.config import get_settings
from src.common.models import AntiEntropyRequest, AntiEntropyResponse, OPCUADataPoint
from src.common.logger import get_logger
from src.node.lamport import LamportClock
from src.node.storage import StorageManager
from src.node.gossip import GossipProtocol

logger = get_logger(__name__)


class AntiEntropyProtocol:
    # implementa il protocollo anti-entropy per eventual consistency e sincronizza periodicamente i dati con i peer del cluster che sono vivi
    
    def __init__(
        self,
        node_id: str,
        lamport_clock: LamportClock,
        storage: StorageManager,
        gossip: GossipProtocol
    ) -> None:
        """
        inizializza il protocollo anti-entropy.
        
        args:
            node_id: identificativo di questo nodo
            lamport_clock: clock logico condiviso
            storage: storage manager per dati locali
            gossip: gossip protocol per conoscere peer alive
        """

        self.node_id = node_id
        self.lamport_clock = lamport_clock
        self.storage = storage
        self.gossip = gossip
        
        # configurazione
        settings = get_settings()
        self.sync_interval = settings.anti_entropy_interval
        self.connection_timeout = settings.connection_timeout
        
        # stato
        self.running = False
        self.session: aiohttp.ClientSession | None = None
        
        # statistiche
        self.total_syncs = 0
        self.total_records_received = 0
        
        logger.info(f"anti-entropy inizializzato per il nodo {node_id}")
    
    
    async def start(self) -> None:
        """avvia il protocollo anti-entropy"""
        self.running = True
        
        # creazione session HTTP
        timeout = aiohttp.ClientTimeout(total=self.connection_timeout)
        connector = aiohttp.TCPConnector(
            limit=10,
            limit_per_host=2,
            force_close=True  # evita keep-alive stale
        )
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        )
        
        logger.info(f"anti-entropy avviato, intervallo {self.sync_interval}s")
        
        # avvia loop in background
        asyncio.create_task(self._sync_loop())
    
    
    async def stop(self) -> None:
        """stop del protocollo anti-entropy"""
        self.running = False
        
        if self.session:
            await self.session.close()
        
        logger.info("anti-entropy stoppato")
    
    
    async def _sync_loop(self) -> None:
        """
        loop principale che esegue sincronizzazione periodica con tutti i peer"""
        
        # attesa inziale
        
        await asyncio.sleep(self.sync_interval)
        
        while self.running:
            try:
                # stabilire quali peer sono alive dal gossip
                perrs_alive = self.gossip.get_alive_peers()
                

                if not perrs_alive:
                    logger.debug("nessun peer alive per anti-entropy")
                    await asyncio.sleep(self.sync_interval)
                    continue
                
                # sincronizzazione con ogni peer
                results = await asyncio.gather(*[self._sync_with_peer(peer.host, peer.port) for peer in perrs_alive], return_exceptions=True)
                
                # conta record sincronizzati
                synced_count = sum(r for r in results if isinstance(r, int))
                if synced_count > 0:
                    logger.info(f"anti-entropy completato: {synced_count} record sincronizzati")
                
                self.total_syncs += 1
                
                # attendi prossimo ciclo
                await asyncio.sleep(self.sync_interval)
                
            except Exception as e:
                logger.error(f"errore nel sync loop: {e}")
                await asyncio.sleep(5)
    
    
    async def _sync_with_peer(self, peer_host: str, peer_port: int) -> int:
        """
        sincronizzazione dati con un singolo peer
        
        args:
            peer_host: hostname del peer
            peer_port: porta del peer
        
        returns:
            numero di record ricevuti dal peer
        """
        
        peer_id = f"{peer_host}:{peer_port}"
        
        try:
            # ottieni il nostro range di lamport clock
            our_min_lc = await self.storage.get_min_lamport_clock()
            our_max_lc = await self.storage.get_max_lamport_clock()
            
            # richiedi i dati che il peer ha e noi non abbiamo
            request = AntiEntropyRequest(
                requester_id=self.node_id,
                min_lamport_clock=our_min_lc,
                max_lamport_clock=our_max_lc
            )
            
            # invia richiesta al peer
            url = f"http://{peer_host}:{peer_port}/internal/anti-entropy/sync"
            
            if not self.session:
                logger.error("session HTTP non inizializzata")
                return 0
            
            async with self.session.post(
                url,
                json=request.model_dump(mode="json")
            ) as response:
                
                if response.status != 200:
                    logger.warning(f"sync con {peer_id} fallito: status {response.status}")
                    return 0
                
                data = await response.json()
                
                #processing della risposta
                records_count = await self._process_sync_response(data, peer_id)
                
                if records_count > 0:
                    logger.debug(f"ricevuti {records_count} record da {peer_id}")
                
                return records_count
                
        except asyncio.TimeoutError:
            logger.warning(f"timeout sync con {peer_id}")
            return 0
        except aiohttp.ClientError as e:
            logger.warning(f"errore connessione a {peer_id}: {e}")
            return 0
        except Exception as e:
            logger.error(f"errore sync con {peer_id}: {e}")
            return 0
    
    
    async def _process_sync_response(self, response_data: dict[str, Any], peer_id: str) -> int:
        """
        - processing dellla risposta di sincronizzazione da un peer
        - inserisce i record mancanti nel nostro storage
        
        args:
            response_data: dati ricevuti dal peer
            peer_id: identificativo del peer
        
        returns:
            numero di record inseriti
        """
        try:
            #parse della risposta
            response = AntiEntropyResponse(**response_data)
            
            if not response.data_points:
                return 0

            #aggiorna lamport clock con quello del peer
            if response.current_max_lc > 0:
                await self.lamport_clock.update(response.current_max_lc)
            
            #estrazione data point
            data_points = response.data_points
            
            # insertid del batch
            inserted = await self.storage.insert_batch(data_points)
            
            self.total_records_received += inserted
            
            return inserted
            
        except Exception as e:
            logger.error(f"errore processing sync response da {peer_id}: {e}")
            return 0
    
    
    async def handle_sync_request(self, request: AntiEntropyRequest) -> dict[str, Any]:
        """
        
        gestion richiesta di sincronizzazione da un altro nodo
        
        args:
            request: richiesta anti-entropy ricevuta
        
        returns:
            risposta con dati mancanti al richiedente
        """
        try:
            # ottieni il lacmport massimo
            our_max_lc = await self.storage.get_max_lamport_clock()
            
            # trova i dati che il richiedente non ha
            # caso 1: se il richiedente ha max_lc < nostro max_lc, inviagli i dati più recenti
            missing_data: list[OPCUADataPoint] = []
            
            if request.max_lamport_clock < our_max_lc:
                # invia dati con LC > al suo max
                missing_data = await self.storage.get_by_lamport_range(
                    min_lc=request.max_lamport_clock + 1,
                    max_lc=our_max_lc
                )
                
                logger.debug(
                    f"inviando {len(missing_data)} record a {request.requester_id} "
                    f"(LC {request.max_lamport_clock + 1}-{our_max_lc})"
                )
            
            # prepara risposta
            response = AntiEntropyResponse(
                responder_id=self.node_id,
                data_points=missing_data,
                current_max_lc=our_max_lc
            )
            
            return response.model_dump(mode="json")
            
        except Exception as e:
            logger.error(f"errore gestione sync request da {request.requester_id}: {e}")
            raise
    
    
    def get_stats(self) -> dict[str, Any]:
        """
        statistiche sul protocollo anti-entropy
        """
        return {
            "total_syncs": self.total_syncs,
            "total_records_received": self.total_records_received,
            "sync_interval_seconds": self.sync_interval,
            "running": self.running
        }