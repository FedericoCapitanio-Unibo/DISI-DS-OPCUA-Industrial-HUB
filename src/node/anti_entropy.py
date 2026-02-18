"""
protocollo anti-entropy perla sincronizzazione dei dati tra nodi. l'idea è che ogni nodo confronta periodicamente i propri dati con gli altri peer e recupera quelli mancanti

"""

import asyncio
from typing import Any

import aiohttp

from src.common.config import get_settings
from src.common.models import AntiEntropyRequest, AntiEntropyResponse, OPCUADataPoint, ServerConfig, ServerConfigSyncRequest, ServerConfigSyncResponse
from src.common.logger import get_logger
from src.node.lamport import LamportClock
from src.node.storage import StorageManager
from src.node.gossip import GossipProtocol

from typing import Any, TYPE_CHECKING


if TYPE_CHECKING:
    from src.node.ingestor import OPCUAIngestor

logger = get_logger(__name__)


class AntiEntropyProtocol:
    # implementa il protocollo anti-entropy per eventual consistency e sincronizza periodicamente i dati con i peer del cluster che sono vivi
    
    def __init__(
        self,
        node_id: str,
        lamport_clock: LamportClock,
        storage: StorageManager,
        gossip: GossipProtocol,
        ingestor: "OPCUAIngestor"
    ) -> None:
        """
        inizializza il protocollo anti-entropy.
        
        args:
            node_id: identificativo di questo nodo
            lamport_clock: clock logico condiviso
            storage: storage manager per dati locali
            gossip: gossip protocol per conoscere peer alive
            ingestor: ingestor per aggiungere/rimuovere server dinamicamente
        """

        self.node_id = node_id
        self.lamport_clock = lamport_clock
        self.storage = storage
        self.gossip = gossip
        self.ingestor = ingestor
        
        # configurazione
        settings = get_settings()
        self.sync_interval = settings.anti_entropy_interval
        self.connection_timeout = settings.connection_timeout
        
        # stato
        self.running = False
        self.session: aiohttp.ClientSession | None = None
        
        # statistiche
        self.n__total_syncs = 0
        self.total_records_received = 0

        # lock per update configurazioni server
        self._server_config_lock = asyncio.Lock()


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
        loop principale che esegue sincronizzazione periodica con tutti i peer
        sincronizza sia data points che configurazioni server
        """
        
        # attesa iniziale
        await asyncio.sleep(self.sync_interval)
        
        while self.running:
            try:
                # stabilire quali peer sono alive dal gossip
                peers_alive = self.gossip.get_alive_peers()
                

                if not peers_alive:
                    logger.debug("nessun peer alive per anti-entropy")
                    await asyncio.sleep(self.sync_interval)
                    continue
                
                # sincronizzazione data points con ogni peer
                data_results = await asyncio.gather(
                    *[self._sync_with_peer(peer.host, peer.port) for peer in peers_alive],
                    return_exceptions=True
                )
                
                # sincronizzazione server configs con ogni peer
                config_results = await asyncio.gather(
                    *[self._sync_server_configs_with_peer(peer.host, peer.port) for peer in peers_alive],
                    return_exceptions=True
                )
                
                # conta record sincronizzati
                data_synced = sum(r for r in data_results if isinstance(r, int))
                configs_synced = sum(r for r in config_results if isinstance(r, int))
                
                if data_synced > 0 or configs_synced > 0:
                    logger.info(
                        f"anti-entropy completato: {data_synced} data points, "
                        f"{configs_synced} server configs sincronizzati"
                    )
                
                self.n__total_syncs += 1
                
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
    

    async def _sync_server_configs_with_peer(self, peer_host: str, peer_port: int) -> int:
        """
        sincronizzazione configurazioni server con un singolo peer
        
        args:
            peer_host: hostname del peer
            peer_port: porta del peer
        
        returns:
            numero di configurazioni ricevute dal peer
        """
        
        peer_id = f"{peer_host}:{peer_port}"
        
        try:
            #ottengo il nostro max lamport clock per server configs
            our_max_lc = await self.storage.get_max_server_config_lc()
            
            # richiedo le config che il peer ha e qesto nodo non ha
            request = ServerConfigSyncRequest(
                requester_id=self.node_id,
                max_lamport_clock=our_max_lc
            )
            
            # invia richiesta al peer
            url = f"http://{peer_host}:{peer_port}/internal/anti-entropy/sync-servers"
            
            if not self.session:
                logger.error("session HTTP non inizializzata")
                return 0
            
            async with self.session.post(
                url,
                json=request.model_dump(mode="json")
            ) as response:
                
                if response.status != 200:
                    logger.warning(f"sync server configs con {peer_id} fallito: status {response.status}")
                    return 0
                
                data = await response.json()
                
                # processing della risposta
                configs_count = await self._process_server_config_sync_response(data, peer_id)
                
                if configs_count > 0:
                    logger.debug(f"ricevute {configs_count} server configs da {peer_id}")
                
                return configs_count
                
        except asyncio.TimeoutError:
            logger.warning(f"timeout sync server configs con {peer_id}")
            return 0
        except aiohttp.ClientError as e:
            logger.warning(f"errore connessione a {peer_id}: {e}")
            return 0
        except Exception as e:
            logger.error(f"errore sync server configs con {peer_id}: {e}")
            return 0


    async def _process_server_config_sync_response(
        self,
        response_data: dict[str, Any],
        peer_id: str
    ) -> int:
        """processing risposta sync server configs"""


        async with self._server_config_lock:

            try:
                response = ServerConfigSyncResponse(**response_data)
                
                if not response.server_configs:
                    return 0

                if response.current_max_lc > 0:
                    await self.lamport_clock.update(response.current_max_lc)
                
                # raggruppo per server_name e prendo solo l'ultimo
                latest_configs: dict[str, ServerConfig] = {}
                for config in response.server_configs:
                    if config.server_name not in latest_configs:
                        latest_configs[config.server_name] = config
                    else:
                        if config.lamport_clock > latest_configs[config.server_name].lamport_clock:
                            latest_configs[config.server_name] = config
                
                
                #prendo le configurazioni nel db
                db_configs: list[ServerConfig] = await self.storage.get_all_server_configs()
                db_server_names = {cfg.server_name for cfg in db_configs}

                configs_applied = 0
                for config in latest_configs.values():
                    
                    # prino check: se c'è già vado avanti 
                    if config.server_name in db_server_names:
                        logger.info(
                            f"server {config.server_name} già nel database, skip..."
                        )
                        continue
                    
                    from src.node.ingestor import OPCUAIngestor
                    ingestor: OPCUAIngestor = self.ingestor
                    
                    # verifico se c'è già
                    already_exists = any(
                        conn.server_name == config.server_name
                        for conn in ingestor.connections
                    )
                    
                    if already_exists:
                        logger.info(f"server '{config.server_name}' già presente, skip")
                        continue
                    
                    #salvo nel database
                    insert = await self.storage.insert_server_config(config)


                    # se non viene inserito correttamente skippo
                    if not insert:
                        continue

                    # aggiungo il server
                    try:
                        success = await ingestor.add_server(
                            endpoint=config.endpoint,
                            server_name=config.server_name
                        )
                        if success:
                            logger.info(
                                f"server '{config.server_name}' aggiunto da sync con {peer_id} "
                                f"(LC={config.lamport_clock})"
                            )
                            configs_applied += 1
                    except Exception as e:
                        logger.debug(f"errore aggiunta {config.server_name}: {e}")

                return configs_applied
                
            except Exception as e:
                logger.error(f"errore processing server config sync response da {peer_id}: {e}")
                return 0
        

    async def handle_server_config_sync_request(
        self,
        request: ServerConfigSyncRequest
    ) -> dict[str, Any]:
        """
        gestione richiesta di sincronizzazione configurazioni server da un altro nodo
        
        args:
            request: richiesta di sync server configs ricevuta
        
        
        ritorn la risposta con configurazioni mancanti al richiedente
        """
        try:
            #ottengo il max lamport clock per server configs
            our_max_lc = await self.storage.get_max_server_config_lc()
            
            #trovo le config che il richiedente non ha
            missing_configs: list[ServerConfig] = []
            
            if request.max_lamport_clock < our_max_lc:
                # invia config con LC > al suo max
                missing_configs = await self.storage.get_server_configs_by_lc_range(
                    min_lc=request.max_lamport_clock + 1,
                    max_lc=our_max_lc
                )
                
                logger.debug(
                    f"inviando {len(missing_configs)} server configs a {request.requester_id} "
                    f"(LC {request.max_lamport_clock + 1}-{our_max_lc})"
                )
            
            # risposta
            response = ServerConfigSyncResponse(
                responder_id=self.node_id,
                server_configs=missing_configs,
                current_max_lc=our_max_lc
            )
            
            return response.model_dump(mode="json")
            
        except Exception as e:
            logger.error(f"errore gestione server config sync request da {request.requester_id}: {e}")
            raise

    
    def get_stats(self) -> dict[str, Any]:
        """
        statistiche sul protocollo anti-entropy
        """
        return {
            "n__total_syncs": self.n__total_syncs,
            "total_records_received": self.total_records_received,
            "sync_interval_seconds": self.sync_interval,
            "running": self.running
        }