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
from src.common.models import OPCUADataPoint, QualityStatus
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

        
        await self.connect()


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
                
                
                self.logger.info(f"batch read completato: {len(data_points)} data points")
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
        

    # async def start_polling(self) -> None:
    #     """avvia loop di polling periodico dei tag"""
        
    #     self.logger.info(f"avvio polling (intervallo = {self.polling_interval}s)")
        
    #     if self._connected:
    #         try:
    #             # leggi tutti i tag
    #             data_points = await self.read_all_tags()
                
    #             # salva in storage
    #             if data_points:
    #                 await self.storage.insert_batch(data_points)
    #                 self.logger.debug(f"salvati {len(data_points)} punti da {self.server_name}")
                
    #             # attendi prossimo ciclo
    #             await asyncio.sleep(self.polling_interval)
            

    #         # se c'è eccezione si riprova dove 5 secondi
    #         except Exception as e:
    #             self.logger.error(f"errore durante polling {self.server_name}: {e}")
    #             await asyncio.sleep(5)
        
    #     else:
    #         self.logger.debug("client disconnesso. skip lettura...")
    #         await asyncio.sleep(5)

    
    # async def connect(self, loop: bool = True):
    #     """
    #     funzione per connettersi al server opcua che riprova fino a che non riesc.
    #     se non riesce c'è un sleep e un retry backoff incrementale

    #     l'idea è quella che allo start ci si provi a connettere e se non si riesce si continua a provare. questa funzione poi
    #     verrà chiamata per provare a riconnettersi quando una connessione viene persa

    #     args:
    #         - loop: se è false si esce dopo il primo ciclo. è quindi utile in fase di setup iniziale

    #     """

    #     # se c'è già un thread che sta tentando di connettersi, esco
    #     if self._connect_lock.locked():
    #         self.self.logger.info("c'è già un thread per la connessione")
    #         return
        

    #     async with self._connect_lock:
    #         attempt = 0

    #         # provo fino a riuscire o fino a superare i tentativi massimi (se impostati)
    #         while True:
    #             try:
    #                 # creo un client nuovo per evitare stati sporchi
    #                 self.client = Client(self.endpoint, watchdog_intervall=60)

    #                 # tento la connessione e verifico la sessione con un ping
    #                 await self.client.connect()
    #                 await self._ping()

    #                 # marco la connessione come attiva e loggo
    #                 self._connected = True
    #                 self.logger.info(f"CONNECTED")

    #                 #browse dei nodi disponibili
    #                 await self._discover_nodes()

    #                 return

    #             except Exception as e:

    #                 self._connected = False

    #                 #se voglio che esca subito (esempio: evitare blocchi all'avvio)
    #                 if not loop:
    #                     raise

    #                 attempt += 1
    #                 self.logger.error(f"connection failed (attempt {attempt}): {e}")

    #                 # se ho un limite di retry e l'ho raggiunto, rilancio l'errore
    #                 if self.conn_retries and attempt >= self.conn_retries:
    #                     raise

    #                 # calcolo un jitter  e attendo
    #                 jitter = random.uniform(-0.2, 0.2) * delay
    #                 await asyncio.sleep(max(0.1, delay + jitter))

    #                 # faccio backoff esponenziale con tetto massimo
    #                 delay = min(delay * 2, 30.0)


    # async def read_all_tags(self) -> list[OPCUADataPoint]:
    #     """
    #     - lettura di tutti i tag disponibili
    #     - creazuibe data point con lamport clock
    #     !ogni tag ottiene un LC univoco incrementale
        
    #     returns:
    #         lista di data point letti
        
    #     """
        
    #     if not self.connected or not self.client:
    #         self.logger.warning(f"tentativo lettura su {self.server_name} non connesso")
    #         return []
        
    #     data_points: list[OPCUADataPoint] = []
    #     timestamp = utc_now()
        
    #     for tag_name, node in self.tag_nodes.items():
    #         try:
    #             # incrementa lamport clock per ogni singolo tag
    #             lc = await self.lamport_clock.tick()
                
    #             value = await node.read_value()
    #             node_id = node.nodeid.to_string()
                
    #             # crea data point con LC univoco
    #             dp = OPCUADataPoint(
    #                 tag=tag_name,
    #                 node_id=node_id,
    #                 value=value,
    #                 timestamp=timestamp,
    #                 quality=QualityStatus.GOOD,
    #                 source_server=self.endpoint,
    #                 lamport_clock=lc
    #             )
                
    #             data_points.append(dp)
                
    #         except Exception as e:
    #             self.logger.error(f"errore lettura tag {tag_name} da {self.server_name}: {e}")
        
    #     if data_points:
    #         self.logger.debug(
    #             f"letti {len(data_points)} tag da {self.server_name} "
    #             f"(LC {data_points[0].lamport_clock}-{data_points[-1].lamport_clock})"
    #         )
        
    #     return data_points
    
    
    # async def start_polling(self) -> None:
    #     """avvia loop di polling periodico dei tag"""
    #     self.logger.info(f"avvio polling su {self.server_name} (intervallo {self.polling_interval}s)")
        
    #     while self.connected:
    #         try:
    #             # leggi tutti i tag
    #             data_points = await self.read_all_tags()
                
    #             # salva in storage
    #             if data_points:
    #                 await self.storage.insert_batch(data_points)
    #                 self.logger.debug(f"salvati {len(data_points)} punti da {self.server_name}")
                
    #             # attendi prossimo ciclo
    #             await asyncio.sleep(self.polling_interval)
            

    #         # se c'è eccezione si riprova dove 5 secondi
    #         except Exception as e:
    #             self.logger.error(f"errore durante polling {self.server_name}: {e}")
    #             await asyncio.sleep(5)
    
    
    # async def reconnect_loop(self) -> None:
    #     """loop per riconnettersi automaticamente in caso di disconnessione"""

    #     while True:
    #         if not self.connected:
    #             self.logger.info(f"tentativo riconnessione a {self.server_name}...")
    #             success = await self.connect()
                
    #             if success:
    #                 # riavvio del polling dopo riconnessione
    #                 asyncio.create_task(self.start_polling())
    #             else:

    #                 await asyncio.sleep(10)
            
    #         await asyncio.sleep(30)


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
        settings = get_settings()
        self.opc_servers = settings.get_opc_servers_list()
        
        #connessioni attive
        self.connections: list[OPCUAConnection] = []
        
        logger.info(f"ingestor inizializzato con {len(self.opc_servers)} server OPC UA: {', '.join(self.opc_servers)}")

        self._scheduler = AsyncIOScheduler()
    
    
    # async def start(self) -> None:
    #     """
    #     avvio dell'ingestor, ovvero connessione e raccolta dati
    #     """
        
    #     if not self.opc_servers:
    #         logger.warning("nessun server OPC UA configurato, ingestor non avviato")
    #         return
        
    #     # crea connessioni per ogni server
    #     for i, endpoint in enumerate(self.opc_servers):
    #         server_name = f"OPCServer{i+1}"
            
    #         connection = OPCUAConnection(
    #             endpoint=endpoint,
    #             server_name=server_name,
    #             lamport_clock=self.lamport_clock,
    #             storage=self.storage
    #         )
            
    #         #tentativo di connettessione
    #         success = await connection.connect()
            
    #         if success:
    #             self.connections.append(connection)
                
    #             #avvia polling in background
    #             asyncio.create_task(connection.start_polling())
                
    #             # avvia loop riconnessione automatica
    #             asyncio.create_task(connection.reconnect_loop())
    #         else:
    #             logger.error(f"impossibile connettersi a {endpoint}, salterò questo server")
        
    #     logger.info(f"ingestor avviato con {len(self.connections)} connessioni attive")


    async def start(self) -> None:
        """
        avvia l'ingestor: connette a tutti i server e inizia raccolta dati.
        """
        
        if not self.opc_servers:
            logger.warning("nessun server OPC UA configurato, ingestor non avviato")
            return
        
        # attesa che i server OPC UA siano pronti # TODO verificare se servono ancora
        # logger.info("attendo 20 secondi per avvio server OPC UA...")
        # await asyncio.sleep(20)


        # crea connessioni per ogni server
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

            # serve per far partire i job dopo n secondi la prima volta
            #applicazione di un intervallo di millisecondi casuale in modo che i job delle diverse connessioni non partano in contemporanea
            some_seconds_delay = utc_now() + timedelta(seconds=15) + timedelta(milliseconds=random.randrange(100, 500, 50))

            #job schedulato ogni n secondi dati dall'intervallo di polling della connessione stessa
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
    

    def get_opcua_server_names(self) -> list[str]:
        """get dei nomi degli ocpua server gestiti dall'ingestor"""

        return [
            connection.server_name
            for connection in self.connections
        ]