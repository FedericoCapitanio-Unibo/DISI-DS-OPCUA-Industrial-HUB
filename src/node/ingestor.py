"""
ingestor per raccogliere dati da server OPC UA multipli

ciò che fa è connettersi ai server opcua, leggere i tag e assegnare lamport clock
"""

import asyncio
from typing import Any
import random
from asyncua import Client, ua
from asyncua.common import Node
from datetime import timedelta

from asyncua.common.subscription import Subscription
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.common.config import get_settings
from src.common.models import OPCUADataPoint, QualityStatus, ServerConfig
from src.common.logger import get_logger
from src.common.utils import utc_now
from src.node.lamport import LamportClock
from src.node.storage import StorageManager


logger = get_logger(__name__)


class OPCUAConnection:

    #nodo che servirà per "pingare" il server opcua e verificare che ci sia connettività
    PING_NODEID = ua.NodeId(ua.ObjectIds.Server_ServerStatus_CurrentTime)
    
    def __init__(
        self,
        endpoint: str,
        server_name: str,
        lamport_clock: LamportClock,
        storage: StorageManager,
        read_retries = 3,   # TODO in ottica di progetto reale questo deve essere parametrico in configurazione
        conn_backoff = 0.5,  # TODO in ottica di progetto reale questo deve essere parametrico in configurazione
        conn_retries = 20   # TODO in ottica di progetto reale questo deve essere parametrico in configurazione
    ) -> None:
        """
        
        args:
            endpoint: url del server (es. opc.tcp://localhost:4840)
            server_name: nome identificativo del server
            lamport_clock: clock logico condiviso del nodo
            storage: storage manager per salvare i dati
            read_retries: quante volte si ritenta la lettura dei tag ad ogni ciclo
            conn_backoff: tempo di attesa ad ogni tentativo di connessione
            conn_retries: numero massimo di volte in cui la classe ritenta autonomamete la connessione al server

        """
        self.endpoint = endpoint
        self.server_name = server_name
        self.lamport_clock = lamport_clock
        self.storage = storage
        
        self.client: Client | None = None
        self.subscription: Subscription | None = None
        self._connected = False
        self._connect_lock = asyncio.Lock()

        # backoof e retry connessione e lettura dati
        self.conn_backoff = conn_backoff
        self.conn_retries = conn_retries
        self.read_retries = read_retries
        
        # mappatura tag name -> node
        self.tag_nodes: dict[str, Node] = {}
        
        # configurazione
        settings = get_settings()
        self.polling_interval = settings.opc_polling_interval
        self.subscription_interval = settings.opc_subscription_interval

        self.logger = get_logger(f"{__name__}[{self.server_name}@{self.endpoint}]") # logger specifico per la connessione così da indentificare subito di quale si parla dai log
        
        self.logger.info(f"connessione inizializzata")
    
    
    # async def connect(self) -> bool:
    #     """
    #     stabilisce la connessione al server opcua. ritorna bool per dire se la connessione è andata a buon fine
    #     """

    #     try:
    #         self.client = Client(self.endpoint)
    #         await self.client.connect()
    #         self.connected = True
            
    #         logger.info(f"connesso a {self.server_name} ({self.endpoint})")
            
    #         # scopri i nodi disponibili
    #         await self._discover_nodes()
            
    #         return True
            
    #     except Exception as e:
    #         logger.error(f"errore connessione a {self.server_name}: {e}")
    #         self.connected = False
    #         return False


    @staticmethod
    def _is_conn_error(exc: Exception) -> bool:
        """
        stabilire se un eccezione è relativa alla connettività o meno
        """
        s = repr(exc)
        keys = (
            "BadSessionIdInvalid",
            "BadSecureChannelIdInvalid",
            "BadSessionClosed",
            "Transport",
            "ConnectionResetError",
            "TimeoutError",
        )
        
        return any(k in s for k in keys)


    @property
    def connected(self) -> bool:
        return self._connected


    async def disconnect(self) -> None:
        """chiude la connessione al server"""
        if self.client and self._connected:
            try:
                if self.subscription:
                    await self.subscription.delete()
                
                await self.client.disconnect()
                self._connected = False
                self.logger.info(f"disconnesso")
                
            except Exception as e:
                self.logger.error(f"errore durante la disconnessione: {e}")
    

    async def _ping(self):
        """
        check di connettività verso il server
        """
        
        assert self.client is not None

        # leggo il CurrentTime del server per validare la sessione
        node = self.client.get_node(self.PING_NODEID)
        _ = await node.read_value()

    
    async def ensure_connected(self):
        """
        check che la connessione sia effettiva
        """
        # se penso di essere connesso faccio un ping altrimenti passo direttamente alla connect
        if self._connected and self.client is not None:
            try:
                await self._ping()
                return
            except Exception:
                pass

        try:
            await self.connect()
        except asyncio.CancelledError:
            pass


    async def connect(self, loop: bool = True):
        """
        funzione per connettersi al server opcua che riprova fino a che non riesc.
        se non riesce c'è un sleep e un retry backoff incrementale

        l'idea è quella che allo start ci si provi a connettere e se non si riesce si continua a provare. questa funzione poi
        verrà chiamata per provare a riconnettersi quando una connessione viene persa

        args:
            - loop: se è false si esce dopo il primo ciclo. è quindi utile in fase di setup iniziale

        """

        # se c'è già un thread che sta tentando di connettersi, esco
        if self._connect_lock.locked():
            self.logger.info("c'è già un thread per la connessione")
            return
        

        async with self._connect_lock:
            attempt = 0

            delay = self.conn_backoff

            # provo fino a riuscire o fino a superare i tentativi massimi (se impostati)
            while True:
                try:
                    # creo un client nuovo per evitare stati sporchi
                    self.client = Client(self.endpoint, watchdog_intervall=60)

                    # tento la connessione e verifico la sessione con un ping
                    await self.client.connect()
                    await self._ping()

                    # marco la connessione come attiva e loggo
                    self._connected = True
                    self.logger.info(f"CONNECTED")

                    #browse dei nodi disponibili
                    await self._discover_nodes()

                    return

                except Exception as e:

                    self._connected = False

                    #se voglio che esca subito (esempio: evitare blocchi all'avvio)
                    if not loop:
                        self.logger.error("connection failed. exit dal connection loop, chiama di nuovo connect() per ritentare la connessione")
                        break

                    attempt += 1
                    self.logger.error(f"connection failed (attempt {attempt}): {e}")

                    # se ho un limite di retry e l'ho raggiunto, rilancio l'errore
                    if self.conn_retries and attempt >= self.conn_retries:
                        raise

                    # calcolo un jitter  e attendo
                    jitter = random.uniform(-0.2, 0.2) * delay
                    await asyncio.sleep(max(0.1, delay + jitter))

                    # faccio backoff esponenziale con tetto massimo
                    delay = min(delay * 2, 30.0)

    
    async def _discover_nodes(self) -> None:
        """
        naviga l'albeatura ocpua per trovare tutti i tag disponibili e crea una mappatura tag_name -> node per accesso rapido.
        """

        # await self.ensure_connected()
        
        # pulizia iniziale
        self.tag_nodes.clear()

        self.logger.info("Start browsing tree")

        try:
            # trova IndustrialPlant in root
            objects = self.client.get_objects_node()
            children = await objects.get_children()
            
            plant_node = None
            for child in children:
                browse_name = await child.read_browse_name()
                if browse_name.Name == "IndustrialPlant":
                    plant_node = child
                    break
            
            if not plant_node:
                self.logger.warning(f"IndustrialPlant non trovato su {self.server_name}")
                return
            
            # scna ricorsivo di tutte le folder
            await self._scan_folder(plant_node, prefix="")
            
            self.logger.info(f"scoperti {len(self.tag_nodes)} tag su {self.server_name}")
            
        except Exception as e:
            self.logger.error(f"errore discovery nodi su {self.server_name}: {e}")
    
    
    async def _scan_folder(self, folder_node: Any, prefix: str) -> None:
        """
        funzione ricorsiva su una folder e registrazione dei tag/nodi che vengnono trovati
        
        args:
            folder_node: nodo folder da scansionare
            prefix: prefisso path per il tag name
        """
        try:
            children = await folder_node.get_children()
            
            for child in children:
                browse_name = await child.read_browse_name()
                node_class = await child.read_node_class()
                
                # costruzione tag name gerarchico
                tag_name = f"{prefix}{browse_name.Name}" if prefix else browse_name.Name
                
                # se è una variabile, viene registrata
                if node_class.name == "Variable":
                    self.tag_nodes[tag_name] = child
                    self.logger.debug(f"registrato tag: {tag_name}")
                
                # se è un folder richiamare di nuovo lo scan della cartella
                elif node_class.name == "Object":
                    await self._scan_folder(child, prefix=f"{tag_name}.")
                    
        except Exception as e:
            self.logger.error(f"errore scansione folder: {e}")
    

    async def read_all_tags(self) -> list[OPCUADataPoint]:
        """
        - lettura di tutti i tag disponibili
        - creazion data point con lamport clock
        !ogni tag ottiene un LC univoco incrementale
        
        returns:
            lista di data point letti
        
        """

        # prima di leggere mi assicuro che la sessione sia valida
        await self.ensure_connected()
        assert self.client is not None


        #tentativi totali di lettura posso fare
        tries = self.read_retries + 1
        last_exc: Exception | None = None


        for ntry in range(1, tries + 1):
            try:
                
                # lettura valori 
                values = await self.client.read_attributes(
                    nodes=list(self.tag_nodes.values()),
                    attr=ua.AttributeIds.Value
                )

                data_points: list[OPCUADataPoint] = []
                batch_timestamp = utc_now()

                #process dei risultati
                for (tag_name, node), data_value in zip(self.tag_nodes.items(), values):
                    
                    lc = await self.lamport_clock.tick()

                    try:
                        
                        node_id = node.nodeid.to_string()
                        
                        # controllo sulla qualità
                        if data_value.StatusCode.is_good():
                            quality = QualityStatus.GOOD
                            value = data_value.Value.Value if data_value.Value else "N/D"
                            timestamp = data_value.SourceTimestamp or batch_timestamp # TODO fare un check se va bene
                        else:
                            quality = QualityStatus.BAD
                            value = "N/D"
                            timestamp = batch_timestamp
                            self.logger.warning(
                                f"lettura tag {tag_name}: quality BAD (status={data_value.StatusCode})"
                            )
                        
                        dp = OPCUADataPoint(
                            tag=tag_name,
                            node_id=node_id,
                            value=value,
                            timestamp=timestamp,
                            quality=quality,
                            source_server=self.endpoint,
                            server_name=self.server_name,
                            lamport_clock=lc
                        )
                        data_points.append(dp)
                        
                    except Exception as e:
                        self.logger.error(f"errore durante il process del tag {tag_name}: {e}")

                        #anche se ho errori, viene comunque aggiunto un data point con BAD quality
                        dp = OPCUADataPoint(
                            tag=tag_name,
                            node_id=node.nodeid.to_string(),
                            value=None,
                            timestamp=batch_timestamp,
                            quality=QualityStatus.BAD,
                            source_server=self.server_name,
                            lamport_clock=lc
                        )
                        data_points.append(dp)
                
                
                self.logger.debug(f"batch read completato: {len(data_points)} data points")
                return data_points

            
            except Exception as e:
                # salvo l'ultima eccezione per rilanciarla alla fine se necessario
                last_exc = e

                # se è un errore di connessione provo prima a riconnettermi
                if self._is_conn_error(exc=e):
                    self._connected = False
                    self.logger.warning(f"read: connection error ({e}) → reconnect (attempt {ntry}/{tries})")
                    try:
                        await self.ensure_connected()
                    except Exception:
                        #se anche la reconnect fallisce, attendo un po' e riprovo
                        await asyncio.sleep(min(1.0 * ntry, 3.0))
                        continue
                else:
                    #per altri errori uso un backoff lineare leggero tra i tentativi
                    self.logger.warning(f"read attempt {ntry}/{tries} failed: {e}")
                    await asyncio.sleep(0.1 * ntry)
                    continue


        # se finisco i tentativi rilancio l'ultima eccezione significativa
        if last_exc is not None:
            raise last_exc
        
        # non dovrei mai arrivare qui, ma proteggo con un errore esplicito
        raise RuntimeError("read_values fallita senza eccezione")


    async def start_polling(self):
        """get dei tag e aggiunta allo storage"""

        try:
            
            # leggi tutti i tag
            data_points = await self.read_all_tags()
                
            # salva in storage
            if data_points:
                await self.storage.insert_batch(data_points)
                self.logger.debug(f"salvati {len(data_points)} punti da {self.server_name}")

        except Exception as e:

            if not self._is_conn_error(exc=e):
                self.logger.error(f"errore durante polling: {e}")
        

class OPCUAIngestor:

    
    def __init__(
        self,
        lamport_clock: LamportClock,
        storage: StorageManager
    ) -> None:
        """
        lamport_clock: clock logico del nodo
        storage: storage manager per persistenza
        """

        self.lamport_clock = lamport_clock
        self.storage = storage
        
        #lettura  configurazione server OPC UA
        # settings = get_settings()
        # self.opc_servers = settings.get_opc_servers_list()
        # lista server OPC UA sarà popolata da database o config
        self.opc_servers: list[str] = []
        
        #connessioni attive
        self.connections: list[OPCUAConnection] = []
        
        logger.info(f"ingestor inizializzato")

        self._scheduler = AsyncIOScheduler()


        # lock per evitare race condition in caso di aggiunta di server 
        self._add_server_lock = asyncio.Lock()
    


    async def start(self) -> None:
        """
        avvia l'ingestor: carica server da database o config e inizia raccolta dati
        """
        
        # carica server da database (hanno priorità)
        server_configs: list[ServerConfig] = await self.storage.get_all_server_configs()
        
        if server_configs:
            # usa server dal database con i loro nomi originali
            # deduplica per endpoint
            seen_endpoints = set()
            unique_configs: list[ServerConfig] = []
            for config in server_configs:
                if config.endpoint not in seen_endpoints:
                    seen_endpoints.add(config.endpoint)
                    unique_configs.append(config)
            
            logger.info(
                f"caricati {len(unique_configs)} server da database: "
                f"{', '.join([c.server_name for c in unique_configs])}"
            )
            
            # crea connessioni usando i nomi dal database
            for config in unique_configs:
                connection = OPCUAConnection(
                    endpoint=config.endpoint,
                    server_name=config.server_name,  # usa il nome dal database
                    lamport_clock=self.lamport_clock,
                    storage=self.storage
                )
                self.connections.append(connection)
                self.opc_servers.append(config.endpoint)
                
                await connection.connect(loop=False)

                # serve per far partire i job dopo n secondi la prima volta
                # applicazione di un intervallo di millisecondi casuale in modo che i job delle diverse connessioni non partano in contemporanea
                some_seconds_delay = utc_now() + timedelta(seconds=15) + timedelta(milliseconds=random.randrange(100, 500, 50))

                # job schedulato ogni n secondi dati dall'intervallo di polling della connessione stessa
                self._scheduler.add_job(
                    func=connection.start_polling,
                    trigger=IntervalTrigger(
                        seconds=connection.polling_interval
                    ),
                    coalesce=True,
                    max_instances=1,
                    misfire_grace_time=5,
                    id=f"job_{connection.server_name}",
                    next_run_time=some_seconds_delay
                )
        
        else:
            # fallback a configurazione da env
            settings = get_settings()
            self.opc_servers = settings.get_opc_servers_list()
            logger.info(f"caricati {len(self.opc_servers)} server da configurazione env")
            
            if not self.opc_servers:
                logger.warning("nessun server OPC UA configurato, ingestor non avviato")
                return
            
            # crea connessioni per ogni server con nomi generati
            for i, endpoint in enumerate(self.opc_servers):
                server_name = f"OPCServer{i+1}"
                
                connection = OPCUAConnection(
                    endpoint=endpoint,
                    server_name=server_name,
                    lamport_clock=self.lamport_clock,
                    storage=self.storage
                )
                self.connections.append(connection)
                
                await connection.connect(loop=False)

                some_seconds_delay = utc_now() + timedelta(seconds=15) + timedelta(milliseconds=random.randrange(100, 500, 50))

                self._scheduler.add_job(
                    func=connection.start_polling,
                    trigger=IntervalTrigger(
                        seconds=connection.polling_interval
                    ),
                    coalesce=True,
                    max_instances=1,
                    misfire_grace_time=5,
                    id=f"job_{connection.server_name}",
                    next_run_time=some_seconds_delay
                )

        self._scheduler.start()
        logger.info(f"ingestor avviato con {len(self.connections)} connessioni")

    
    async def stop(self) -> None:

        logger.info("stop ingestor...")

        # stop scheduler
        # if self._scheduler.running:
        #     self._scheduler.shutdown(wait=False)

        # stop client opcua
        for connection in self.connections:
            await connection.disconnect()
        
        self.connections.clear()
        logger.info("ingestor fermato")
    
    
    def get_status(self) -> dict[str, Any]:
        """
        stato dell'istanza dell'ingestor
        """
        return {
            "total_servers": len(self.opc_servers),
            "connected_servers": sum(1 for c in self.connections if c.connected),
            "connections": [
                {
                    "server_name": c.server_name,
                    "endpoint": c.endpoint,
                    "connected": c.connected,
                    "tags_count": len(c.tag_nodes)
                }
                for c in self.connections
            ]
        }
    

    def get_opcua_servers_names(self) -> list[str]:
        """get dei nomi degli ocpua server gestiti dall'ingestor"""

        return [
            connection.server_name
            for connection in self.connections
        ]
    

    async def add_server(self, endpoint: str, server_name: str) -> bool:
        """
        aggiunge un nuovo server OPC UA a runtime
        
        args:
            endpoint: url del server, ad esempio opc.tcp://opc-server-4:4843
            server_name: nome identificativo del server di aggiungere
        
        ritorna un bool che dice se effettivamente è stato aggiunto
        """


        # evitare race condition
        async with self._add_server_lock:

            # verifico che non esista già
            for conn in self.connections:
                # if conn.endpoint == endpoint:
                #     logger.info(f"server con endpoint '{endpoint}' già presente, skip aggiunta")
                #     return False
                if conn.server_name == server_name:
                    logger.info(f"server con nome '{server_name}' già presente, skip aggiunta")
                    return False
            
            #verifico anche nella lista dell'istanza
            # if endpoint in self.opc_servers:
            #     logger.info(f"endpoint '{endpoint}' già nella lista opc_servers, skip aggiunta")
            #     return False
            
            # creo la connessione nuova
            connection = OPCUAConnection(
                endpoint=endpoint,
                server_name=server_name,
                lamport_clock=self.lamport_clock,
                storage=self.storage
            )
            
            job_id = f"job_{connection.server_name}"

            #check sul job dato che il nome lo prende da lì
            if self._scheduler.get_job(job_id=job_id) is not None:
                logger.warning(f"c'è già un job per '{server_name}'({job_id}), pertanto è già stato aggiunto")
                return False

            # # tenta connessione
            # await connection.connect(loop=False)
            
            # aggiungo job per il polling allo scheduler
            some_seconds_delay = utc_now() + timedelta(seconds=5) + timedelta(milliseconds=random.randrange(100, 500, 50))

            self._scheduler.add_job(
                func=connection.start_polling,
                trigger=IntervalTrigger(seconds=connection.polling_interval),
                coalesce=True,
                max_instances=1,
                misfire_grace_time=5,
                id=job_id,
                next_run_time=some_seconds_delay
            )

            # aggiungi alla lista
            self.connections.append(connection)
            self.opc_servers.append(endpoint)
                
            logger.info(f"server '{server_name}'({endpoint}) aggiunto dinamicamente")
            return True


    def get_opcua_servers(self) -> list[dict[str, Any]]:
        """

        get di tutti i server OPC UA configurati con dettagli
        """
        return [
            {
                "server_name": conn.server_name,
                "endpoint": conn.endpoint,
                "connected": conn.connected,
                "tags_count": len(conn.tag_nodes)
            }
            for conn in self.connections
        ]