import random
import pickle
import asyncio
import logging

from protocol import KademliaProtocol
from utils import digest
from storage import ForgetfulStorage
from node import Node
from crawling import ValueSpiderCrawl
from crawling import NodeSpiderCrawl

log = logging.getLogger(__name__)


class Server:
    protocol_class = KademliaProtocol

    def __init__(self, ksize=20, alpha=3, node_id=None, storage=None):
        # Inicializa el servidor con ksize, alpha, ID de nodo y almacenamiento opcional
        self.ksize = ksize
        self.alpha = alpha
        self.storage = storage or ForgetfulStorage()
        self.node = Node(node_id or digest(random.getrandbits(255)))
        self.transport = None
        self.protocol = None
        self.refresh_loop = None
        self.save_state_loop = None

    def stop(self):
        # Detiene el servidor y cancela los loops de refresco y guardado de estado
        if self.transport is not None:
            self.transport.close()

        if self.refresh_loop:
            self.refresh_loop.cancel()

        if self.save_state_loop:
            self.save_state_loop.cancel()

    def _create_protocol(self):
        # Crea una instancia del protocolo Kademlia
        return self.protocol_class(self.node, self.storage, self.ksize)

    async def listen(self, port, interface="0.0.0.0"):
        # Inicia el servidor en la interfaz y puerto especificados
        loop = asyncio.get_event_loop()
        listen = loop.create_datagram_endpoint(
            self._create_protocol, local_addr=(interface, port)
        )
        log.info("Node %i listening on %s:%i", self.node.long_id, interface, port)
        self.transport, self.protocol = await listen
        self.refresh_table()

    def refresh_table(self):
        # Refresca la tabla de enrutamiento periódicamente
        log.debug("Refreshing routing table")
        asyncio.ensure_future(self._refresh_table())
        loop = asyncio.get_event_loop()
        self.refresh_loop = loop.call_later(3600, self.refresh_table)

    async def _refresh_table(self):
        # Actualiza la tabla de enrutamiento buscando nodos refrescados
        results = []
        for node_id in self.protocol.get_refresh_ids():
            node = Node(node_id)
            nearest = self.protocol.router.find_neighbors(node, self.alpha)
            spider = NodeSpiderCrawl(
                self.protocol, node, nearest, self.ksize, self.alpha
            )
            results.append(spider.find())

        await asyncio.gather(*results)

        for dkey, value in self.storage.iter_older_than(3600):
            await self.set_digest(dkey, value)

    def bootstrappable_neighbors(self):
        # Devuelve los vecinos disponibles para el bootstrap
        neighbors = self.protocol.router.find_neighbors(self.node)
        return [tuple(n)[-2:] for n in neighbors]

    async def bootstrap(self, addrs):
        # Realiza el bootstrap del nodo utilizando las direcciones iniciales
        log.debug("Attempting to bootstrap node with %i initial contacts", len(addrs))
        cos = list(map(self.bootstrap_node, addrs))
        gathered = await asyncio.gather(*cos)
        nodes = [node for node in gathered if node is not None]
        spider = NodeSpiderCrawl(
            self.protocol, self.node, nodes, self.ksize, self.alpha
        )
        return await spider.find()

    async def bootstrap_node(self, addr):
        # Realiza el bootstrap con un solo nodo
        result = await self.protocol.ping(addr, self.node.id)
        return Node(result[1], addr[0], addr[1]) if result[0] else None

    async def get(self, key):
        # Busca un valor en la red
        log.info("Looking up key %s", key)
        dkey = digest(key)
        if self.storage.get(dkey) is not None:
            return self.storage.get(dkey)
        node = Node(dkey)
        nearest = self.protocol.router.find_neighbors(node)
        if not nearest:
            log.warning("There are no known neighbors to get key %s", key)
            return None
        spider = ValueSpiderCrawl(self.protocol, node, nearest, self.ksize, self.alpha)
        return await spider.find()

    async def set(self, key, value):
        # Almacena un valor en la red
        if not check_dht_value_type(value):
            raise TypeError("Value must be of type int, float, bool, str, or bytes")
        log.info("setting '%s' = '%s' on network", key, value)
        dkey = digest(key)
        return await self.set_digest(dkey, value)

    async def set_digest(self, dkey, value):
        # Almacena un valor digest en la red
        node = Node(dkey)
        nearest = self.protocol.router.find_neighbors(node)
        if not nearest:
            log.warning("There are no known neighbors to set key %s", dkey.hex())
            return False

        spider = NodeSpiderCrawl(self.protocol, node, nearest, self.ksize, self.alpha)
        nodes = await spider.find()
        log.info("setting '%s' on %s", dkey.hex(), list(map(str, nodes)))
        biggest = max([n.distance_to(node) for n in nodes])
        if self.node.distance_to(node) < biggest:
            self.storage[dkey] = value
        results = [self.protocol.call_store(n, dkey, value) for n in nodes]
        return any(await asyncio.gather(*results))

    def save_state(self, fname):
        # Guarda el estado del nodo en un archivo
        log.info("Saving state to %s", fname)
        data = {
            "ksize": self.ksize,
            "alpha": self.alpha,
            "id": self.node.id,
            "neighbors": self.bootstrappable_neighbors(),
        }
        if not data["neighbors"]:
            log.warning("No known neighbors, so not writing to cache.")
            return
        with open(fname, "wb") as file:
            pickle.dump(data, file)

    @classmethod
    async def load_state(cls, fname, port, interface="0.0.0.0"):
        # Carga el estado del nodo desde un archivo
        log.info("Loading state from %s", fname)
        with open(fname, "rb") as file:
            data = pickle.load(file)
        svr = Server(data["ksize"], data["alpha"], data["id"])
        await svr.listen(port, interface)
        if data["neighbors"]:
            await svr.bootstrap(data["neighbors"])
        return svr

    def save_state_regularly(self, fname, frequency=600):
        # Guarda el estado del nodo periódicamente
        self.save_state(fname)
        loop = asyncio.get_event_loop()
        self.save_state_loop = loop.call_later(
            frequency, self.save_state_regularly, fname, frequency
        )


def check_dht_value_type(value):
    # Verifica si el tipo de valor es válido para la DHT
    typeset = [int, float, bool, str, bytes]
    return type(value) in typeset
