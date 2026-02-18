"""
layer di persistenza per i dati OPC UA usando SQLAlchemy + SQLite.
gestisce salvataggio, query e sincronizzazione dei data point con lamport clock.
"""

from datetime import datetime
from pathlib import Path
import asyncio

from sqlalchemy import String, Integer, Float, DateTime, Index, select, func, and_
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.common.config import get_settings
from src.common.models import OPCUADataPoint, QualityStatus
from src.common.logger import get_logger

from src.common.utils import  utc_now

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.common.models import ServerConfig

logger = get_logger(__name__)


class Base(DeclarativeBase):
    """
    base class per tutti i modelli SQLAlchemy. se devo aggiunge peculiarità a tutte è più facile perchè lo farò solo qui"""
    
    pass


class DataPointRecord(Base):
    """
    tabella per salvare i data point OPC UA con lamport clock
    - ogni record rappresenta un campionamento di un tag
    """
    __tablename__ = "data_points"
    
    # chiave primaria
    lamport_clock: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    #identificatori per i nodi opcua
    tag: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # valore del nodo. di default è un float ma l'eventuale conversione verrà gestita dall'applicazione
    value: Mapped[float] = mapped_column(Float, nullable=False)
    
    # altri dati del nodo
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    quality: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default=QualityStatus.GOOD.value
    )
    source_server: Mapped[str] = mapped_column(String(255), nullable=False)

    server_name: Mapped[str] = mapped_column(String(255), nullable=False)
    
    #indici composti per query comuni
    __table_args__ = (
        Index("idx_tag_timestamp", "tag", "timestamp"),
        Index("idx_tag_lc", "tag", "lamport_clock"),
    )
    
    def __repr__(self) -> str:
        return f"<DataPoint(lc={self.lamport_clock}, tag='{self.tag}', value={self.value})>"


class ServerConfigRecord(Base):
    """
    tabella per salvare le configurazioni dei server OPC UA
    - ogni record rappresenta l'aggiunta di un server
    - sincronizzato tra nodi tramite anti-entropy
    """
    __tablename__ = "server_configs"
    
    # chiave primaria composta: server_name + lamport_clock
    server_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    lamport_clock: Mapped[int] = mapped_column(Integer, primary_key=True)
    
    # dati configurazione
    endpoint: Mapped[str] = mapped_column(String(255), nullable=False)
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    # RIMUOVI: action: Mapped[str] = mapped_column(String(20), nullable=False)
    
    # indice per query
    __table_args__ = (
        Index("idx_server_lc", "server_name", "lamport_clock"),
    )
    
    def __repr__(self) -> str:
        return f"<ServerConfig(server='{self.server_name}', lc={self.lamport_clock})>"


class StorageManager:
    """
    gestisce tutte le operazioni sul database SQLite
    
    """
    
    def __init__(self, db_path: Path | None = None) -> None:
        """
            db_path: path del file SQLite, se None usa config
        """
        if db_path is None:
            settings = get_settings()
            db_path = settings.get_db_path()
        
        self.db_path = db_path
        self.engine: AsyncEngine | None = None
        self.session_maker: async_sessionmaker[AsyncSession] | None = None


        # lock per inserimento conifguraizoni dei server
        self._insert_server_config_lock = asyncio.Lock()
        
        logger.info(f"storage manager inizializzato con db: {self.db_path}")
    
    
    async def initialize(self) -> None:
        """
        creazione del database engine e delle tabelle
        !! chiamare questo metodo prima di usare lo storage
        """

        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # creazione engine
        database_url = f"sqlite+aiosqlite:///{self.db_path}"
        self.engine = create_async_engine(
            database_url,
            echo=False,             #da mettee a true per debug o dev
            pool_pre_ping=True,
        )
        
        #istanza della sessione
        self.session_maker = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        # creazione tabelle
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        logger.info("database inizializzato e tabelle create")
    
    
    async def close(self) -> None:
        """chiusura connessioni al database"""
        if self.engine:
            try:
                await self.engine.dispose()
                logger.info("connessioni database chiuse")
            except asyncio.CancelledError:
                # ignoro lo spam si messaggi continuo in chiusura
                logger.debug("CancelledError durante chiusura engine (normale durante shutdown)")
            except Exception as e:
                logger.error(f"errore chiusura engine: {e}")
    
    
    async def insert_data_point(self, data_point: OPCUADataPoint) -> None:
        """
        salvataggio singolo data point nel database.
        
        args:
            data_point -> dati da salvare
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato, chiama initialize() prima")
        
        async with self.session_maker() as session:
            record = DataPointRecord(
                lamport_clock=data_point.lamport_clock,
                tag=data_point.tag,
                node_id=data_point.node_id,
                value=float(data_point.value),
                timestamp=data_point.timestamp,
                quality=data_point.quality.value,
                source_server=data_point.source_server,
                server_name=data_point.server_name
            )
            
            session.add(record)
            await session.commit()
    
    
    async def insert_batch(self, data_points: list[OPCUADataPoint]) -> int:
        """
        funzione per salvataggo data point mutilpli in batch per performance migliori.
        ignora duplicati (stesso lamport_clock) senza errore
        
        args:
            data_points: lista di punti dati da salvare
        
        returns:
            numero di record effettivamente inseriti
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        # se non è stata passata nemmeno un elemtno
        if not data_points:
            return 0
        
        async with self.session_maker() as session:
            records = [
                DataPointRecord(
                    lamport_clock=dp.lamport_clock,
                    tag=dp.tag,
                    node_id=dp.node_id,
                    value=float(dp.value),
                    timestamp=dp.timestamp,
                    quality=dp.quality.value,
                    source_server=dp.source_server,
                    server_name=dp.server_name
                )
                for dp in data_points
            ]
            
            # insert
            inserted = 0
            for record in records:
                try:
                    session.add(record)
                    await session.flush()
                    inserted += 1
                except Exception:
                    # duplicato o altro errore, skippa
                    await session.rollback()
                    continue
            
            await session.commit()
            logger.debug(f"inseriti {inserted}/{len(data_points)} record in batch")
            return inserted
    

    async def srv_exists(self, server_name: str) -> bool:

        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = select(func.count()).select_from(DataPointRecord).where(DataPointRecord.server_name == server_name)
            result = await session.execute(query)
            count = result.scalar_one()
            
            return count > 0


    async def list_all_servers(self) -> list[str]:

        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = select(DataPointRecord).distinct()
            result = await session.execute(query)
            count = result.scalars().all()
            
            return count > 0

    
    async def get_latest_tag(self, server_name: str, tag: str) -> OPCUADataPoint | None:
        """
        ottenere l'ultimo valore di un tag specifico
        
        args:
            tag: nome del tag da cercare        
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = (
                select(DataPointRecord)
                # .where(DataPointRecord.tag == tag)
                .where(
                    and_(
                        DataPointRecord.tag == tag,
                        DataPointRecord.server_name == server_name
                    )
                )
                .order_by(DataPointRecord.lamport_clock.desc())
                .limit(1)
            )
            
            result = await session.execute(query)
            record = result.scalar_one_or_none()
            
            if record:
                return self._record_to_model(record)
            return None
    
    
    async def get_tag_history(
        self,
        server_name: str,
        tag: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 1000
    ) -> list[OPCUADataPoint]:
        """
        get dello storico valori di un tag in un range temporale
        
        args:
            tag: nome del tag
            start_time: timestamp iniziale (None = dall'inizio)
            end_time: timestamp finale (None = fino ad ora)
            limit: massimo numero di record da restituire
        
        i dati restituiti sono ordinati secondo lamport clock
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = select(DataPointRecord).where(DataPointRecord.server_name == server_name, DataPointRecord.tag == tag)
            
            # filtri opzionali
            if start_time:
                query = query.where(DataPointRecord.timestamp >= start_time)
            if end_time:
                query = query.where(DataPointRecord.timestamp <= end_time)
            
            query = query.order_by(DataPointRecord.lamport_clock.asc()).limit(limit)
            
            result = await session.execute(query)
            records = result.scalars().all()
            
            return [self._record_to_model(r) for r in records]
    
    
    async def get_by_lamport_range(
        self,
        min_lc: int,
        max_lc: int
    ) -> list[OPCUADataPoint]:
        """
        ottenre tutti i data point in un range di lamport clock, è utile per anti-entrpy       
        args:
            min_lc: lamport clock minimo
            max_lc: lamport clock massimo

            min_lc e max_lc sono entrambi inclusi
        

        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = (
                select(DataPointRecord)
                .where(
                    and_(
                        DataPointRecord.lamport_clock >= min_lc,
                        DataPointRecord.lamport_clock <= max_lc
                    )
                )
                .order_by(DataPointRecord.lamport_clock.asc())
            )
            
            result = await session.execute(query)
            records = result.scalars().all()
            
            return [self._record_to_model(r) for r in records]
    
    
    async def get_max_lamport_clock(self) -> int:
        """
        ottenere il massimo lamport clock presente nel database
        ritorna il massimo LC o 0 se database vuoto
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = select(func.max(DataPointRecord.lamport_clock))
            result = await session.execute(query)
            max_lc = result.scalar_one_or_none()
            
            return max_lc if max_lc is not None else 0
    
    
    async def get_min_lamport_clock(self) -> int:
        """
        ottenere il minimo lamport clock presente nel database.
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = select(func.min(DataPointRecord.lamport_clock))
            result = await session.execute(query)
            min_lc = result.scalar_one_or_none()
            
            return min_lc if min_lc is not None else 0
    
    
    async def count_total_records(self) -> int:
        """
        conta il numero totale di record nel database
        
        returns:
            numero di data point salvati
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = select(func.count()).select_from(DataPointRecord)
            result = await session.execute(query)
            count = result.scalar_one()
            
            return count
        
    
    async def count_records(self, server_name: str) -> int:
        """
        conta il numero totale di record nel database per unsolo server
        
        returns:
            numero di data point salvati
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = select(func.count()).select_from(DataPointRecord).where(DataPointRecord.server_name == server_name)
            result = await session.execute(query)
            count = result.scalar_one()
            
            return count
    
    
    async def get_all_tags(self) -> list[dict[str, str]]:
        """
        ottiene la lista di tutti i tag
        
        returns:
            lista di dict
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = select(
                DataPointRecord.server_name, 
                DataPointRecord.tag
            ).distinct()
            result = await session.execute(query)
            rows = result.all()# lista di tuple (source_server, tag)
            
            return [
                {"server": server_name, "tag": tag}
                for server_name, tag in rows
            ]
        

    async def get_all_tags_from_srv(self, server_name: str) -> list[str]:
        
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")

        async with self.session_maker() as session:
            query = (
                select(
                    DataPointRecord.tag
                )
                .where(DataPointRecord.server_name == server_name)
                .distinct()
            )

            result = await session.execute(query)
    
            return list(result.scalars().all())

    
    def _record_to_model(self, record: DataPointRecord) -> OPCUADataPoint:
        """
        utile per conversione
        """
        return OPCUADataPoint(
            tag=record.tag,
            node_id=record.node_id,
            value=record.value,
            timestamp=record.timestamp,
            quality=QualityStatus(record.quality),
            source_server=record.source_server,
            server_name=record.server_name,
            lamport_clock=record.lamport_clock
        )
    

    async def insert_server_config(self, config: "ServerConfig") -> bool:
        """salva configurazione server nel database"""
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self._insert_server_config_lock, self.session_maker() as session:
            record = ServerConfigRecord(
                server_name=config.server_name,
                endpoint=config.endpoint,
                lamport_clock=config.lamport_clock,
                node_id=config.node_id,
                timestamp=utc_now()
            )
            
            try:
                session.add(record)
                await session.commit()
                logger.info(f"salvata config server {config.server_name} (LC={config.lamport_clock})")
            except Exception as e:
                await session.rollback()
                if "UNIQUE constraint failed" in str(e):
                    logger.debug(f"config server {config.server_name} (LC={config.lamport_clock}) già presente, skip")
                else:
                    logger.error(f"errore salvataggio config server: {e}")

                return False
            
            return True


    async def get_all_server_configs(self) -> list["ServerConfig"]:
        """
        ottiene tutte le configurazioni server dal database
        ritorna solo l'ultima per ogni server (LC più alto)
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        from src.common.models import ServerConfig
        
        async with self.session_maker() as session:
            query = (
                select(ServerConfigRecord)
                .order_by(ServerConfigRecord.lamport_clock.desc())
            )
            
            result = await session.execute(query)
            records = result.scalars().all()
            
            # si fa raggruppameto per server_name e prendo solo il più recente
            latest_by_server: dict[str, ServerConfigRecord] = {}
            for record in records:
                if record.server_name not in latest_by_server:
                    latest_by_server[record.server_name] = record
            
            #converto in ServerConfig (tutti sono server attivi)
            configs = [
                ServerConfig(
                    server_name=record.server_name,
                    endpoint=record.endpoint,
                    lamport_clock=record.lamport_clock,
                    node_id=record.node_id
                )
                for record in latest_by_server.values()
            ]
            
            logger.debug(f"get_all_server_configs ritorna {len(configs)} server")
            return configs


    async def get_server_configs_by_lc_range(
        self,
        min_lc: int,
        max_lc: int
    ) -> list["ServerConfig"]:
        """ottiene configurazioni server in un range di lamport clock"""
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        from src.common.models import ServerConfig
        
        async with self.session_maker() as session:
            query = (
                select(ServerConfigRecord)
                .where(
                    and_(
                        ServerConfigRecord.lamport_clock >= min_lc,
                        ServerConfigRecord.lamport_clock <= max_lc
                    )
                )
                .order_by(ServerConfigRecord.lamport_clock.asc())
            )
            
            result = await session.execute(query)
            records = result.scalars().all()
            
            return [
                ServerConfig(
                    server_name=r.server_name,
                    endpoint=r.endpoint,
                    lamport_clock=r.lamport_clock,
                    node_id=r.node_id
                )
                for r in records
            ]


    async def get_max_server_config_lc(self) -> int:
        """
        ottiene il massimo lamport clock delle configurazioni server
        
        returns:
            massimo LC o 0 se nessuna config presente
        """
        if not self.session_maker:
            raise RuntimeError("storage non inizializzato")
        
        async with self.session_maker() as session:
            query = select(func.max(ServerConfigRecord.lamport_clock))
            result = await session.execute(query)
            max_lc = result.scalar_one_or_none()
            
            return max_lc if max_lc is not None else 0