""" configurazione dell'applicazione che viene caricata da variabili d'ambiente"""

from pathlib import Path
from typing import Literal
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    configurazione del nodo HUB

    i valori sono caricati da variabili d'ambiente con eventuale fallback ad un dato valore di default
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # identificazione nodo
    node_id: str = Field(
        default="node-default",
        description="identificativo univoco del nodo nel cluster"
    )
    
    node_port: int = Field(
        default=8000,
        description="porta su cui esporre le API"
    )
    
    # peer del cluster
    peers: str = Field(
        default="",
        description="lista peer separati da virgola"
    )
    
    # server OPC UA da monitorare
    opc_servers: str = Field(
        default="",
        description="lista endpoint OPC UA separati da virgola"
    )
    
    # intervalli temporali (in secondi)
    gossip_interval: int = Field(
        default=5,
        description="intervallo tra heartbeat gossip in secondi"
    )
    
    anti_entropy_interval: int = Field(
        default=10,
        description="intervallo tra cicli anti-entropy in secondi"
    )
    

    failure_timeout: int = Field(
        default=15,
        description="timeout dopo cui un nodo è considerato down (secondi)"
    )
    
    # storage
    storage_path: Path = Field(
        default=Path("data"),
        description="directory per database SQLite"
    )
    
    db_name: str = Field(
        default="hub_storage.db",
        description="nome file database SQLite"
    )
    
    # sicurezza
    jwt_secret: str = Field(
        default="change-me",
        description="secret key per firma JWT token"
    )
    
    jwt_algorithm: str = Field(
        default="HS256",
        description="algoritmo per JWT"
    )
    
    jwt_expiration_minutes: int = Field(
        default=60,
        description="durata validità token JWT in minuti"
    )
    
    # logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="livello di logging"
    )
    
    log_format: Literal["json", "text"] = Field(
        default="json",
        description="formato output log"
    )
    
    log_file: Path | None = Field(
        default=None,
        description="path opzionale per log su file"
    )
    
    # prestazioni
    max_batch_size: int = Field(
        default=1000,
        description="dimensione massima batch per operazioni storage"
    )
    
    connection_timeout: int = Field(
        default=10,
        description="timeout connessioni HTTP in secondi"
    )
    
    # OPC UA

    # TODO mettere un controllo per numero troppo basso(forse overhead sui server)
    opc_subscription_interval: int = Field(
        default=1000,
        description="intervallo subscription OPC UA in millisecondi"
    )
    
    opc_polling_interval: int = Field(
        default=5,
        description="intervallo polling OPC UA in secondi se subscription non è disponibile"
    )


    def get_peers_list(self) -> list[tuple[str, int]]:
        """
        parse della stringa peers in lista di (host, port)
        
        returns:
            lista di tuple hostname + porta dei peer
        """
        if not self.peers:
            return []
        
        peers_list = []
        for peer in self.peers.split(","):
            peer = peer.strip()
            if ":" in peer:
                host, port_str = peer.rsplit(":", 1)
                try:
                    port = int(port_str)
                    peers_list.append((host, port))
                except ValueError:
                    # qullie che saranno malformati sono ignorati
                    continue
        
        return peers_list


    def get_opc_servers_list(self) -> list[str]:
        """
        parse della stringa opc_servers in lista di endpoint.
        
        returns:
            lista di endpoint OPC UA del tipo opc.tpc://....

        """
        if not self.opc_servers:
            return []
        
        return [s.strip() for s in self.opc_servers.split(",") if s.strip()]


    def get_db_path(self) -> Path:
        """ ottiene il path assoluto del database SQLite """
        self.storage_path.mkdir(parents=True, exist_ok=True)
        return self.storage_path / self.db_name


# istanza globale singleton delle settings
_settings: Settings | None = None


def get_settings() -> Settings:
    """ ottiene l'istanza delle settings o la carica al primo accesso """
    
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reload_settings() -> Settings:
    """
    ricarica la configurazione  (per testare)
    """
    global _settings
    _settings = Settings()
    return _settings