"""
implementazione del lamport logical clock per ordinamento causale degli eventi.
ogni nodo mantiene un contatore locale che viene incrementato ad ogni evento
e sincronizzato quando riceve messaggi da altri nodi.
"""

import asyncio
from typing import Any


class LamportClock:
    '''
    def: clock logico di lamport per stabilire ordinamento causale tra eventi distribuiti
    - è thread-safe tramite asyncio.Lock per gestire accessi concorrenti
    '''
    
    def __init__(self, initial_value: int = 0) -> None:
        """
        inizializzazione clock con valore iniziale.
        
        """
        self._counter: int = initial_value
        self._lock: asyncio.Lock = asyncio.Lock()
    
    
    async def tick(self) -> int:
        """
        incremento del clock locale di 1 (evento interno al nodo)
        funzione che va chiamata ogni volta che il nodo fa qualcosa di rilevante, tipo leggere un dato opcua, ecc.
        """
        async with self._lock:
            self._counter += 1
            return self._counter
    
    
    async def update(self, received_clock: int) -> int:
        """
        aggiorna il clock locale quando riceviamo un messaggio da un altro nodo.
        applica la regola di lamport, quindi LC = max(LC_local, LC_received) + 1
        
        args:
            received_clock: valore del clock ricevuto da un altro nodo
        
        
        ritorna il nuovo valore del clock locale dopo l'aggiornamento
        """
        async with self._lock:
            self._counter = max(self._counter, received_clock) + 1
            return self._counter
    
    
    async def get_time(self) -> int:
        """
        get del valore corrente del clock senza modificarlo
        """
        async with self._lock:
            return self._counter
    
    
    async def set_time(self, new_value: int) -> None:
        """
        imposta forzatamente il clock a un valore specifico(più per test)
        
        args:
            new_value: nuovo valoreda impostare come clock
        """
        async with self._lock:
            self._counter = new_value
    
    
    async def send_event(self) -> int:
        """
        serve per preparare il clock per l'invio di un messaggio ad altri nodi. restitusice il valore del clock da includere nel messaggio in uscita
        """
        return await self.tick()
    
    
    async def receive_event(self, received_clock: int) -> int:
        """
        aggiorna il clock alla ricezione di un messaggio. stessa roba della funzione update ma inserendo "event" nel nome fa più chiarezza logica
        """
        return await self.update(received_clock)
    
    
    def __repr__(self) -> str:
        return f"LamportClock(counter={self._counter})"
    
    
    def __str__(self) -> str:
        return str(self._counter)


class LamportTimestamp:
    """
    timestamp logico con node_id per ordinamento totale. serve per risolvere conflitti quando due eventi hanno lo stesso LC.
    """
    
    def __init__(self, clock_value: int, node_id: str) -> None:
        """        
        values:
            clock_value: valore del lamport clock
            node_id: identificativo del nodo che ha generato l'evento
        """
        self.clock_value = clock_value
        self.node_id = node_id
    
    
    def __lt__(self, other: "LamportTimestamp") -> bool:
        """
        confronto: questo timestamp è precedente all'altro? prima confronto sul clock, poi sul node_id per ordine
        """
        if self.clock_value != other.clock_value:
            return self.clock_value < other.clock_value
        return self.node_id < other.node_id
    
    
    def __le__(self, other: "LamportTimestamp") -> bool:
        """confronto: precedente o uguale"""
        return self < other or self == other
    
    
    def __eq__(self, other: Any) -> bool:
        """uguaglianza: stesso clock e stesso nodo"""
        if not isinstance(other, LamportTimestamp):
            return False
        return self.clock_value == other.clock_value and self.node_id == other.node_id
    
    
    def __hash__(self) -> int:
        """hash per usare in set e dict"""
        return hash((self.clock_value, self.node_id))
    
    
    def __repr__(self) -> str:
        return f"LamportTimestamp(clock={self.clock_value}, node='{self.node_id}')"
    
    
    def __str__(self) -> str:
        return f"{self.clock_value}@{self.node_id}"
    
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "clock_value": self.clock_value,
            "node_id": self.node_id
        }
    
    

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LamportTimestamp":
        return cls(
            clock_value=data["clock_value"],
            node_id=data["node_id"]
        )


def compare_timestamps(ts1: LamportTimestamp, ts2: LamportTimestamp) -> int:
    """
    confronta due timestamp logici e restituisce -1, 0, o 1 """
    if ts1 < ts2:
        return -1
    elif ts1 == ts2:
        return 0
    else:
        return 1