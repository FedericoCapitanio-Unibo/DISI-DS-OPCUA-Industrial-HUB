
### MODELLI E FORMATO DATI

from datetime import datetime
from enum import Enum
from typing import Any
from pydantic import BaseModel, Field, ConfigDict

from src.common.utils import utc_now


class QualityStatus(str, Enum):
    """stato qualitativo dei dati OPC UA"""
    
    GOOD = "GOOD"
    BAD = "BAD"
    UNCERTAIN = "UNCERTAIN"


class OPCUADataPoint(BaseModel):
    """
    rappresenta un singolo punto dati letto da un server OPCUA che include timestamp logico (lamport clock) per ordinamento causale
    """

    model_config = ConfigDict(frozen=False)
    
    tag: str = Field(..., description="nome leggibile del tag")
    node_id: str = Field(..., description="node id OPCUA")
    value: float | int | str | bool = Field(..., description="valore letto dal server")
    timestamp: datetime = Field(default_factory=utc_now, description="timestamp UTC di acquisizione")
    quality: QualityStatus = Field(default=QualityStatus.GOOD, description="qualità del dato")
    source_server: str = Field(..., description="endpoint del server OPC UA sorgente")
    server_name: str = Field(..., description="nome del server OPC UA sorgente")
    lamport_clock: int = Field(..., description="timestamp logico di lamport per ordinamento causale")


class OPCUAServerConfig(BaseModel):
    """configurazione per la connessione a un server OPC UA"""

    model_config = ConfigDict(frozen=True)
    
    name: str = Field(..., description="nome identificativo del server")
    endpoint: str = Field(..., description="endpoint OPC UA")
    namespace_index: int = Field(default=2, description="indice dek namespace da utilizzare")
    tag_mappings: dict[str, str] = Field(
        default_factory=dict,
        description="mappatura tra node_id -> tag leggibile"
    )


class NodeInfo(BaseModel):
    """informazioni su un nodo del cluster distribuito"""
    model_config = ConfigDict(frozen=False)
    
    node_id: str = Field(..., description="identificativo univoco del nodo")
    host: str = Field(..., description="hostname o IP")
    port: int = Field(..., description="porta delle API")
    is_alive: bool = Field(default=True, description="stato del nodo (rilevato via gossip)")
    last_seen: datetime = Field(default_factory=utc_now, description="ultimo heartbeat ricevuto")
    lamport_clock: int = Field(default=0, description="ultimo lamport clock noto")


class GossipMessage(BaseModel):
    """messaggio scambiato nel protocollo gossip per failure detection"""

    model_config = ConfigDict(frozen=False)
    
    sender_id: str = Field(..., description="id del nodo mittente")
    message_type: str = Field(..., description="tipo messaggio che potrà 'ping', 'ack', 'suspect', 'alive'")
    lamport_clock: int = Field(..., description="lamport clock del mittente")
    payload: dict[str, Any] = Field(default_factory=dict, description="dati aggiuntivi opzionali")
    timestamp: datetime = Field(default_factory=utc_now)


class AntiEntropyRequest(BaseModel):
    """richiesta di sincronizzazione anti-entropy tra nodi"""
    model_config = ConfigDict(frozen=False)
    
    requester_id: str = Field(..., description="id del nodo richiedente")
    min_lamport_clock: int = Field(..., description="minimo lamport clock posseduto dal richiedente")
    max_lamport_clock: int = Field(..., description="massimo lamport clock posseduto dal richiedente")


class AntiEntropyResponse(BaseModel):
    """risposta contenente dati mancanti per sincronizzazione"""
    model_config = ConfigDict(frozen=False)
    
    responder_id: str = Field(..., description="id del nodo rispondente")
    data_points: list[OPCUADataPoint] = Field(default_factory=list, description="punti dati mancanti")
    current_max_lc: int = Field(..., description="massimo LC posseduto dal rispondente")


class APITokenPayload(BaseModel):
    """payload contenuto nel JWT token per autenticazione  api """
    model_config = ConfigDict(frozen=False)
    
    sub: str = Field(..., description="client_id")
    exp: datetime = Field(..., description="tempo di scandenza token")
    iat: datetime = Field(default_factory=utc_now, description="quando è stato rilasciato")
    scopes: list[str] = Field(default_factory=list, description="permessi e scope del token")


class HealthCheckResponse(BaseModel):
    """ risposta per endpoint di health check """
    model_config = ConfigDict(frozen=False)
    
    status: str = Field(..., description="stato del nodo che potrà essere 'healthy', 'degraded', 'unhealthy'")
    node_id: str = Field(..., description="identificativo del nodo")
    
    lamport_clock: int = Field(..., description="lamport clock corrente")
    uptime_seconds: float = Field(..., description="tempo di uptime in secondi")
    connected_opc_servers: int = Field(..., description="numero di server opcua connessi")
    active_peers: int = Field(..., description="numero di peer attivi rilevati")
    storage_records_count: int = Field(..., description="numero di record nello storage locale")


class AddOPCServerRequest(BaseModel):
    """richiesta per aggiungere un server OPC UA dinamicamente"""
    
    #TODO valutare se lasciare solo uno dei due come identificativo
    server_name: str = Field(
        ...,
        description="Nome identificativo del server (es. OPCServer4)",
        examples=["OPCServer4"]
    )
    endpoint: str = Field(
        ...,
        description="URL del server OPC UA (es. opc.tcp://opc-server-4:4843)",
        examples=["opc.tcp://opc-server-4:4843"]
    )


class ServerConfig(BaseModel):
    """configurazione di un server OPC UA con timestamp logico"""
    model_config = ConfigDict(frozen=False)
    
    server_name: str = Field(..., description="nome identificativo del server")
    endpoint: str = Field(..., description="endpoint OPC UA (es. opc.tcp://opc-server-4:4843)")
    lamport_clock: int = Field(..., description="timestamp logico di quando è stato aggiunto/rimosso")
    node_id: str = Field(..., description="id del nodo che ha effettuato l'azione")


class ServerConfigSyncRequest(BaseModel):
    """richiesta di sincronizzazione configurazioni server"""
    model_config = ConfigDict(frozen=False)
    
    requester_id: str = Field(..., description="id del nodo richiedente")
    max_lamport_clock: int = Field(..., description="massimo lamport clock delle config possedute dal richiedente")


class ServerConfigSyncResponse(BaseModel):
    """
    risposta con configurazioni server mancanti
    """
    
    model_config = ConfigDict(frozen=False)
    
    responder_id: str = Field(..., description="id del nodo rispondente")
    server_configs: list[ServerConfig] = Field(default_factory=list, description="configurazioni server mancanti")
    current_max_lc: int = Field(..., description="massimo LC delle config possedute dal rispondente")