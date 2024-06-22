import random
import asyncio
import logging

from rpcudp.protocol import RPCProtocol

from node import Node
from routing import RoutingTable
from utils import digest

log = logging.getLogger(__name__)


class KademliaProtocol(RPCProtocol):
    def __init__(self, source_node, storage, ksize):
        # Inicializa el protocolo Kademlia con el nodo fuente, almacenamiento y tamaño k
        RPCProtocol.__init__(self)
        self.router = RoutingTable(self, ksize, source_node)
        self.storage = storage
        self.source_node = source_node

    def get_refresh_ids(self):
        # Obtiene IDs para buscar y mantener los buckets actualizados
        ids = []
        for bucket in self.router.lonely_buckets():
            rid = random.randint(*bucket.range).to_bytes(20, byteorder="big")
            ids.append(rid)
        return ids

    def rpc_stun(self, sender):
        # Maneja una solicitud STUN devolviendo el remitente
        return sender

    def rpc_ping(self, sender, nodeid):
        # Maneja una solicitud PING y devuelve el ID del nodo fuente
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_if_new(source)
        return self.source_node.id

    def rpc_store(self, sender, nodeid, key, value):
        # Maneja una solicitud STORE y almacena el valor
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_if_new(source)
        log.debug(
            "got a store request from %s, storing '%s'='%s'", sender, key.hex(), value
        )
        self.storage[key] = value
        return True

    def rpc_find_node(self, sender, nodeid, key):
        # Maneja una solicitud FIND_NODE y devuelve los vecinos más cercanos
        log.info("finding neighbors of %i in local table", int(nodeid.hex(), 16))
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_if_new(source)
        node = Node(key)
        neighbors = self.router.find_neighbors(node, exclude=source)
        return list(map(tuple, neighbors))

    def rpc_find_value(self, sender, nodeid, key):
        # Maneja una solicitud FIND_VALUE y devuelve el valor si está almacenado
        source = Node(nodeid, sender[0], sender[1])
        self.welcome_if_new(source)
        value = self.storage.get(key, None)
        if value is None:
            return self.rpc_find_node(sender, nodeid, key)
        return {"value": value}

    async def call_find_node(self, node_to_ask, node_to_find):
        # Llama a FIND_NODE en otro nodo
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.find_node(address, self.source_node.id, node_to_find.id)
        return self.handle_call_response(result, node_to_ask)

    async def call_find_value(self, node_to_ask, node_to_find):
        # Llama a FIND_VALUE en otro nodo
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.find_value(address, self.source_node.id, node_to_find.id)
        return self.handle_call_response(result, node_to_ask)

    async def call_ping(self, node_to_ask):
        # Llama a PING en otro nodo
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.ping(address, self.source_node.id)
        return self.handle_call_response(result, node_to_ask)

    async def call_store(self, node_to_ask, key, value):
        # Llama a STORE en otro nodo
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.store(address, self.source_node.id, key, value)
        return self.handle_call_response(result, node_to_ask)

    def welcome_if_new(self, node):
        # Añade un nodo a la tabla de enrutamiento si es nuevo
        if not self.router.is_new_node(node):
            return

        log.info("never seen %s before, adding to router", node)
        for key, value in self.storage:
            keynode = Node(digest(key))
            neighbors = self.router.find_neighbors(keynode)
            if neighbors:
                last = neighbors[-1].distance_to(keynode)
                new_node_close = node.distance_to(keynode) < last
                first = neighbors[0].distance_to(keynode)
                this_closest = self.source_node.distance_to(keynode) < first
            if not neighbors or (new_node_close and this_closest):
                asyncio.ensure_future(self.call_store(node, key, value))
        self.router.add_contact(node)

    def handle_call_response(self, result, node):
        # Maneja la respuesta de una llamada RPC
        if not result[0]:
            log.warning("no response from %s, removing from router", node)
            self.router.remove_contact(node)
            return result

        log.info("got successful response from %s", node)
        self.welcome_if_new(node)
        return result
