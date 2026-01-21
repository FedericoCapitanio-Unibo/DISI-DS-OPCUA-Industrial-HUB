import asyncio
import random
import math
import os
from datetime import datetime
from typing import Any

from asyncua import Server, ua
from asyncua.common.methods import uamethod

from src.common.logger import setup_logger

logger = setup_logger(__name__, level="INFO", json_format=False)


class OPCUASimulator:
    """ simulatore di un server OPC UA industriale"""
    
    def __init__(
        self,
        server_name: str = "PlantSimulator",
        endpoint: str = "opc.tcp://0.0.0.0:4840",
        namespace_index: int = 2
    ) -> None:
        """
        args:
            server_name: nome identificativo del server
            endpoint: endpoint su cui esporre il server
            namespace_index: indice del namespace da usare
        """
        self.server_name = server_name
        self.endpoint = endpoint
        self.namespace_index = namespace_index
        
        self.server: Server | None = None
        self.namespace_uri = f"http://industrial-hub.local/{server_name}"
        self.nodes: dict[str, Any] = {}
        
        # parametri pe rla simulazione
        self.simulation_running = False
        self.update_interval = 1.0  # secondi ogni aggiornamento dei tag
        
        logger.info(f"simulatore inizializzato: {server_name} su {endpoint}")
    
    
    async def initialize(self) -> None:
        """preparazione del server OPC UA e creazione della struttura dei nodi"""
        
        self.server = Server()
        await self.server.init()
        
        #configurazione server
        self.server.set_endpoint(self.endpoint)
        self.server.set_server_name(self.server_name)
        
        #registrazione del namespace
        idx = await self.server.register_namespace(self.namespace_uri)
        self.namespace_index = idx
        
        # creazione struttura nodi
        await self._create_node_structure()
        
        logger.info(f"server OPC UA inizializzato su {self.endpoint}")
    
    
    async def _create_node_structure(self) -> None:
        """crea la gerarchia di nodi OPC UA con tag simulati.
            l'idea è quella di dividere in zone come se fosse effettivamente una macchina da packaging e
            andare a valorizzare per ognuna di essere delle grandezze fisiche oltre ad altri valori come il contatore dei pezzi prodotti,
            le valvole ecc.
        """
        if not self.server:
            raise RuntimeError("server non inizializzato")
        
        # get del root objects
        objects = self.server.get_objects_node()
        
        # creazione folder principale per rappresentare il plant
        plant_folder = await objects.add_folder(self.namespace_index, "IndustrialPlant")
        
        # zona 1: temperatura e umidità
        zone1 = await plant_folder.add_folder(self.namespace_index, "Zone1")
        
        self.nodes["temperature_zone_1"] = await zone1.add_variable(
            self.namespace_index,
            "Temperature",
            20.0,  # valore iniziale
            ua.VariantType.Float
        )
        await self.nodes["temperature_zone_1"].set_writable()
        
        self.nodes["humidity_zone_1"] = await zone1.add_variable(
            self.namespace_index,
            "Humidity",
            50.0,
            ua.VariantType.Float
        )
        await self.nodes["humidity_zone_1"].set_writable()
        
        # zona 2: pressione e livello
        zone2 = await plant_folder.add_folder(self.namespace_index, "Zone2")
        
        self.nodes["pressure_zone_2"] = await zone2.add_variable(
            self.namespace_index,
            "Pressure",
            1.0,
            ua.VariantType.Float
        )
        await self.nodes["pressure_zone_2"].set_writable()
        
        self.nodes["level_zone_2"] = await zone2.add_variable(
            self.namespace_index,
            "Level",
            75.0,
            ua.VariantType.Float
        )
        await self.nodes["level_zone_2"].set_writable()
        
        # linea produzione: flusso e velocità
        production_line = await plant_folder.add_folder(self.namespace_index, "ProductionLine")
        
        self.nodes["flow_rate"] = await production_line.add_variable(
            self.namespace_index,
            "FlowRate",
            50.0,
            ua.VariantType.Float
        )
        await self.nodes["flow_rate"].set_writable()
        
        self.nodes["conveyor_speed"] = await production_line.add_variable(
            self.namespace_index,
            "ConveyorSpeed",
            100.0,
            ua.VariantType.Float
        )
        await self.nodes["conveyor_speed"].set_writable()
        
        # valvole e attuatori (valori booleani)
        actuators = await plant_folder.add_folder(self.namespace_index, "Actuators")
        
        self.nodes["valve_1_open"] = await actuators.add_variable(
            self.namespace_index,
            "Valve1Open",
            True,
            ua.VariantType.Boolean
        )
        await self.nodes["valve_1_open"].set_writable()
        
        self.nodes["pump_running"] = await actuators.add_variable(
            self.namespace_index,
            "PumpRunning",
            True,
            ua.VariantType.Boolean
        )
        await self.nodes["pump_running"].set_writable()
        
        # contatori
        counters = await plant_folder.add_folder(self.namespace_index, "Counters")
        
        self.nodes["production_count"] = await counters.add_variable(
            self.namespace_index,
            "ProductionCount",
            0,
            ua.VariantType.Int32
        )
        await self.nodes["production_count"].set_writable()
        
        logger.info(f"creati {len(self.nodes)} nodi simulati")
    
    
    async def start(self) -> None:
        """avvia il server OPC UA e la simulazione"""
        if not self.server:
            raise RuntimeError("server non inizializzato, chiama initialize() prima")
        
        # avvia server
        async with self.server:
            logger.info(f"server OPC UA avviato su {self.endpoint}")
            
            # avvia simulazione valori
            self.simulation_running = True
            await self._run_simulation()
    
    
    async def _run_simulation(self) -> None:
        """
            loop principale che aggiorna i valori simulati periodicamente.
            ogni secondo si fa un ciclo di aggiornamento, ogni tag avrà poi la sua "reale" frequenze di aggiornamnento e
            il suo valore verrà aggioranto eventualmente ogni tot cicli
        """
        iteration = 0
        
        while self.simulation_running:
            try:
                await self._update_simulated_values(iteration)
                iteration += 1
                await asyncio.sleep(self.update_interval)
                
            except Exception as e:
                logger.error(f"errore durante simulazione: {e}")
                await asyncio.sleep(1)
    
    
    async def _update_simulated_values(self, iteration: int) -> None:
        """
        aggiorna tutti i valori simulati con pattern realistici e
        usa funzioni trigonometriche e random per variare i valori in modo che i diversi server opcua
        che verranno poi usati per la simulazione siano più casuali
        """

        # tempo in secondi dall'inizio
        t = iteration * self.update_interval
        
        # temperatura che varia tra 18 e 28 gradi con un po' di rumore
        base_temp = 23.0
        temp_variation = 5.0 * math.sin(t / 10.0)
        temp_noise = random.uniform(-0.5, 0.5)
        temperature = base_temp + temp_variation + temp_noise
        # usa ua.Variant per forzare il tipo Float corretto
        await self.nodes["temperature_zone_1"].write_value(
            ua.Variant(round(temperature, 2), ua.VariantType.Float)
        )
        
        # umidità che varia tra 40 e 60%
        base_humidity = 50.0
        humidity_variation = 10.0 * math.sin(t / 20.0)
        humidity_noise = random.uniform(-1.0, 1.0)
        humidity = base_humidity + humidity_variation + humidity_noise
        await self.nodes["humidity_zone_1"].write_value(
            ua.Variant(round(humidity, 2), ua.VariantType.Float)
        )
        
        # pressione che varia tra 0.8 e 1.4 (ipoteticamente parliamo di bar)
        base_pressure = 1.1
        pressure_variation = 0.3 * math.sin(t / 15.0)
        pressure_noise = random.uniform(-0.05, 0.05)
        pressure = base_pressure + pressure_variation + pressure_noise
        await self.nodes["pressure_zone_2"].write_value(
            ua.Variant(round(pressure, 3), ua.VariantType.Float)
        )
        
        # livello che varia tra 60 e 90%
        base_level = 75.0
        level_variation = 15.0 * math.sin(t / 25.0)
        level_noise = random.uniform(-2.0, 2.0)
        level = base_level + level_variation + level_noise
        await self.nodes["level_zone_2"].write_value(
            ua.Variant(round(level, 2), ua.VariantType.Float)
        )
        
        # flusso che varia in modo random stando circa sui 50 (in caso reale L(litri)/min)
        base_flow = 50.0
        flow_noise = random.uniform(-5.0, 5.0)
        flow = base_flow + flow_noise
        await self.nodes["flow_rate"].write_value(
            ua.Variant(round(flow, 2), ua.VariantType.Float)
        )
        
        # velocità nastro che cambia ogni 10 secondi
        if iteration % 10 == 0:
            speeds = [80.0, 100.0, 120.0]
            speed = random.choice(speeds)
            await self.nodes["conveyor_speed"].write_value(
                ua.Variant(speed, ua.VariantType.Float)
            )
        
        #valvole che cambiano stato casualmente
        if iteration % 15 == 0:
            valve_state = random.choice([True, False])
            await self.nodes["valve_1_open"].write_value(valve_state)
        
        #pompa -> boolean per indicare se è accesa o spenta
        if iteration % 30 == 0:
            pump_state = random.random() > 0.4
            await self.nodes["pump_running"].write_value(pump_state)
        
        #contatore produzione che incrementa periodicamente di un valore casuale tra 1 e 9 compresi
        if iteration % 5 == 0:
            current_count = await self.nodes["production_count"].read_value()
            await self.nodes["production_count"].write_value(
                ua.Variant(current_count + random.choice(range(1, 10)), ua.VariantType.Int32)
            )
        

        # ogni 10 iterazioni una print
        if iteration % 10 == 0:
            logger.debug(f"valori aggiornati (iteration {iteration})")
    
    
    async def stop(self) -> None:
        """ferma la simulazione e il server"""
        self.simulation_running = False
        logger.info("server OPC UA stoppato")


async def main() -> None:
    
    # configurazione prese dall'ambente
    server_name = os.getenv("SERVER_NAME", "PlantSimulator")
    server_port = int(os.getenv("SERVER_PORT", "4840"))
    endpoint = os.getenv("ENDPOINT", f"opc.tcp://0.0.0.0:{server_port}")
    
    simulator = OPCUASimulator(
        server_name=server_name,
        endpoint=endpoint
    )
    
    await simulator.initialize()
    
    logger.info(f"avvio server {server_name}...")
    logger.info("premi Ctrl+C per fermare")
    
    try:
        await simulator.start()
    except KeyboardInterrupt:
        logger.info("interruzione ricevuta, chiusura server...")
        await simulator.stop()
    except Exception as e:
        logger.error(f"errore fatale: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())