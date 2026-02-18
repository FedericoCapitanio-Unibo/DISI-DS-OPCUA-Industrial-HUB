
import asyncio
from datetime import timedelta
from typing import Any

import aiohttp

from src.common.config import get_settings
from src.common.models import GossipMessage, NodeInfo
from src.common.logger import get_logger
from src.common.utils import utc_now
from src.node.lamport import LamportClock

logger = get_logger(__name__)

###########
##### - protocollo gossip per failure detection tra nodi del cluster.
##### - ogni nodo invia ping periodici ai peer e rileva quando qualcuno va down.
###########


class GossipProtocol:
    
    def __init__(
        self,
        node_id: str,
        node_port: int,
        lamport_clock: LamportClock
    ) -> None:
        """
            node_id: identificativo univoco di questo nodo
            node_port: porta su cui questo nodo espone le api
            lamport_clock: clock logico condiviso del nodo
        """

        self.node_id = node_id
        self.node_port = node_port
        self.lamport_clock = lamport_clock
        
        # carica configurazione
        settings = get_settings()
        self.peers_config = settings.get_peers_list()
        self.gossip_interval = settings.gossip_interval
        self.failure_timeout = settings.failure_timeout
        
        # stato dei peer
        self.peers: dict[str, NodeInfo] = {}
        self._initialize_peers()
        
        
        self.running = False    # flag per loop del fatto che sti andando
        
        self.session: aiohttp.ClientSession | None = None
        
        logger.info(f"gossip inizializzato per nodo {node_id} con {len(self.peers)} peer")
    
    
    def _initialize_peers(self) -> None:
        """
        inizializza la lista dei peer dalla configurazione
        """
        
        for host, port in self.peers_config:
            peer_id = f"{host}:{port}"
            
            self.peers[peer_id] = NodeInfo(
                node_id=peer_id,
                host=host,
                port=port,
                is_alive=True,          # assunzione che inizialmente il nodo sia alive
                last_seen=utc_now(),
                lamport_clock=0
            )
    
    
    async def start(self) -> None:
        """avvio il protocollo gossip"""
        self.running = True
        
        # crea session HTTP con connector configurato per ridurre errori
        timeout = aiohttp.ClientTimeout(total=5)
        connector = aiohttp.TCPConnector(
            limit=10,               # max numero di connessioni totali
            limit_per_host=2,       # max numeor di connessioni per host
            ttl_dns_cache=300,      
            force_close=True        # chiusara connessioni dopo ogni request (evita keep-alive stale)
        )
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        )
        
        logger.info(f"gossip avviato, intervallo {self.gossip_interval}s")
        
        # avvia loop gossip in background
        asyncio.create_task(self._gossip_loop())
        asyncio.create_task(self._failure_detection_loop())
    
    
    async def stop(self) -> None:
        """ferma il protocollo gossip"""
        self.running = False
        
        if self.session:
            await self.session.close()
        
        logger.info("gossip fermato")
    
    
    async def _gossip_loop(self) -> None:
        """loop principale che invia ping periodici a tutti i peer"""
        while self.running:
            try:
                # invia ping a tutti i peer
                await asyncio.gather(*[self._send_ping(peer_id) for peer_id in self.peers.keys()], return_exceptions=True)
                
                # attendi prossimo ciclo
                await asyncio.sleep(self.gossip_interval)
                
            except Exception as e:
                logger.error(f"errore nel gossip loop: {e}")
                await asyncio.sleep(1)
    
    
    async def _send_ping(self, peer_id: str) -> None:
        """
        invio di un  ping a un peer specifico
        
        args:
            peer_id: identificativo del peer destinatario
        """
        peer = self.peers.get(peer_id)
        if not peer:
            return
        
        try:
            # incremento lamport clock per invio messaggio
            lc = await self.lamport_clock.send_event()
            
            # costruisci messaggio ping
            ping_msg = GossipMessage(
                sender_id=self.node_id,
                message_type="ping",
                lamport_clock=lc,
                payload={"timestamp": utc_now().isoformat()}
            )
            
            # invio richiesta HTTP
            url = f"http://{peer.host}:{peer.port}/internal/gossip/ping"
            
            if not self.session:
                logger.error("session HTTP non inizializzata")
                return
            
            async with self.session.post(
                url,
                json=ping_msg.model_dump(mode="json"),
                timeout=aiohttp.ClientTimeout(total=3)
            ) as response:
                
                if response.status == 200:
                    # ricevuto ack
                    ack_data = await response.json()
                    await self._handle_ack(peer_id, ack_data)
                else:
                    logger.warning(f"ping a {peer_id} fallito: status {response.status}")
                    
        except asyncio.TimeoutError:
            logger.debug(f"timeout ping verso {peer_id}")
        except aiohttp.ClientError as e:
            # se è "Server disconnected"  è normale perchè probabilmente è connessione keep-alive chiusa
            if "Server disconnected" in str(e):
                logger.debug(f"connessione a {peer_id} chiusa dal server, riproverà")
            else:
                logger.warning(f"errore connessione a {peer_id}: {e}")
        except Exception as e:
            logger.error(f"errore invio ping a {peer_id}: {e}")
    
    
    async def _handle_ack(self, peer_id: str, ack_data: dict[str, Any]) -> None:
        """
        gestisce la ricezione di un ack da un peer
        
        args:
            peer_id: id del peer che ha risposto
            ack_data: dati contenuti nell'ack

        """
        peer = self.peers.get(peer_id)
        if not peer:
            return
        
        try:

            # aggiornamento lamport clock con quello ricevuto
            received_lc = ack_data.get("lamport_clock", 0)
            await self.lamport_clock.receive_event(received_lc)
            
            # aggiornamento delle stato del peer
            peer.is_alive = True
            peer.last_seen = utc_now()
            peer.lamport_clock = received_lc
            
            logger.debug(f"ricevuto ack da {peer_id} (LC={received_lc})")
            
        except Exception as e:
            logger.error(f"errore gestione ack da {peer_id}: {e}")
    
    
    async def handle_incoming_ping(self, ping_msg: GossipMessage) -> dict[str, Any]:
        """
        gestione di un ping ricevuto da un altro nodo        
        args:
            ping_msg: messaggio ping ricevuto
        
        
        ritorna un dizionario con risposta ack
        
        """
        try:
            # aggiornamento lamport clock
            await self.lamport_clock.receive_event(ping_msg.lamport_clock)
            
            # preparazione risposta ack
            lc = await self.lamport_clock.send_event()
            
            ack = {
                "sender_id": self.node_id,
                "message_type": "ack",
                "lamport_clock": lc,
                "timestamp": utc_now().isoformat()
            }
            
            logger.debug(f"risposto ack a {ping_msg.sender_id}")
            
            return ack
            
        except Exception as e:
            logger.error(f"errore gestione ping da {ping_msg.sender_id}: {e}")
            raise
    
    
    async def _failure_detection_loop(self) -> None:
        """loop che controlla periodicamente se qualche peer è andato down"""
        
        while self.running:
            try:
                await asyncio.sleep(self.gossip_interval)
                
                now = utc_now()
                timeout_threshold = timedelta(seconds=self.failure_timeout)
                
                for peer_id, peer in self.peers.items():
                    time_since_last_seen = now - peer.last_seen
                    
                    # se il peer non viene visto da troppo tempo viene marcato come DOWN
                    if time_since_last_seen > timeout_threshold:
                        if peer.is_alive:
                            logger.warning(
                                f"peer {peer_id} marcato come DOWN "
                                f"(silenzio da {time_since_last_seen.total_seconds():.0f}s)"
                            )
                            peer.is_alive = False

            except Exception as e:
                logger.error(f"errore nel failure detection loop: {e}")
    
    
    def get_alive_peers(self) -> list[NodeInfo]:
        """
        ottenimento la lista dei peer attualmente vivi
        
        returns:
            info dei nodi attivi

        """

        return [peer for peer in self.peers.values() if peer.is_alive]
    
    
    def get_dead_peers(self) -> list[NodeInfo]:
        """
        ottnere  la lista dei peer attualmente down
        
        returns:
            lista di peer non raggiungibili
        """
        return [peer for peer in self.peers.values() if not peer.is_alive]
    
    
    def get_peer_status(self) -> dict[str, Any]:
        """
        get dello stato completo di tutti i peer
        returns:
            dizionario con statistiche e dettagli peerper peer
        """
        alive_peers = self.get_alive_peers()
        dead_peers = self.get_dead_peers()
        
        return {
            "total_peers": len(self.peers),
            "alive_peers": len(alive_peers),
            "dead_peers": len(dead_peers),
            "peers": [
                {
                    "node_id": peer.node_id,
                    "host": peer.host,
                    "port": peer.port,
                    "is_alive": peer.is_alive,
                    "last_seen": peer.last_seen.isoformat(),
                    "lamport_clock": peer.lamport_clock
                }
                for peer in self.peers.values()
            ]
        }
    
    
    async def get_max_peer_lamport_clock(self) -> int:
        """
        ottemere il massimo lamport clock tra tutti i peer che sono vivi
        """
        alive_peers = self.get_alive_peers()
        
        if not alive_peers:
            return 0
        
        return max(peer.lamport_clock for peer in alive_peers)