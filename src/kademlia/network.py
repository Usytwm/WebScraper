import argparse
import hashlib
import logging
import eventlet
from flask import Flask
from flask_socketio import SocketIO, emit
from threading import Thread
import time
from collections import OrderedDict

eventlet.monkey_patch()

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constantes
K = 20  # Número de contactos en un bucket
ALPHA = 3  # Número de solicitudes concurrentes en la red


# Función hash para los IDs de nodos
def hash_function(data):
    return hashlib.sha1(data.encode("utf-8")).hexdigest()


# Definición de clases para el protocolo Kademlia
class Node:
    def __init__(self, id, ip, port, is_bootstrap=False):
        self.id = id
        self.ip = ip
        self.port = port
        self.is_bootstrap = is_bootstrap

    def distance(self, other):
        return int(self.id, 16) ^ int(other.id, 16)

    @classmethod
    def from_dict(cls, data):
        return cls(
            data["id"], data["ip"], data["port"], data.get("is_bootstrap", False)
        )

    def to_dict(self):
        return {
            "id": self.id,
            "ip": self.ip,
            "port": self.port,
            "is_bootstrap": self.is_bootstrap,
        }


class KBucket:
    def __init__(self, range_start, range_end):
        self.range_start = range_start
        self.range_end = range_end
        self.nodes = OrderedDict()

    def add_node(self, node):
        if node.id in self.nodes:
            del self.nodes[node.id]
        elif len(self.nodes) >= K:
            self.nodes.popitem(last=False)
        self.nodes[node.id] = node
        logger.info(f"Node {node.id} added to bucket.")

    def remove_node(self, node_id):
        if node_id in self.nodes:
            del self.nodes[node_id]
            logger.info(f"Node {node_id} removed from bucket.")

    def get_closest_nodes(self, target_id, count=K):
        nodes = list(self.nodes.values())
        nodes.sort(key=lambda node: node.distance(Node(target_id, "", 0)))
        return nodes[:count]


class RoutingTable:
    def __init__(self, node_id):
        self.node_id = node_id
        self.buckets = [KBucket(0, 2**160)]

    def find_bucket(self, node_id):
        for bucket in self.buckets:
            if bucket.range_start <= int(node_id, 16) < bucket.range_end:
                return bucket
        return None

    def update(self, node):
        bucket = self.find_bucket(node.id)
        bucket.add_node(node)

    def find_closest_nodes(self, node_id, count=K):
        bucket = self.find_bucket(node_id)
        return bucket.get_closest_nodes(node_id, count)


class Kademlia:
    def __init__(self, node):
        self.node = node
        self.routing_table = RoutingTable(node.id)
        self.storage = {}

    def store(self, key, value):
        self.storage[key] = value
        logger.info(f"Value stored: {key} -> {value}")

    def find_value(self, key):
        return self.storage.get(key)

    def find_node(self, node_id):
        return self.routing_table.find_closest_nodes(node_id)

    def join_network(self, bootstrap_nodes):
        for bootstrap_node in bootstrap_nodes:
            self.routing_table.update(bootstrap_node)

        closest_nodes = self.iterative_find_node(self.node)
        for node in closest_nodes:
            self.routing_table.update(node)

        logger.info(f"Node {self.node.id} joined the network using bootstrap nodes.")

    def iterative_find_node(self, node):
        shortlist = self.find_node(node.id)
        already_contacted = set()
        while shortlist:
            batch = shortlist[:ALPHA]
            shortlist = shortlist[ALPHA:]
            responses = []
            for n in batch:
                if n.id not in already_contacted:
                    already_contacted.add(n.id)
                    found_nodes = self.rpc_find_node(n, node.id)
                    responses.extend(found_nodes)
                    for found_node in found_nodes:
                        self.routing_table.update(found_node)
            shortlist.extend(responses)
            shortlist.sort(key=lambda n: n.distance(Node(node.id, "", 0)))
            shortlist = shortlist[:K]
        return shortlist

    def iterative_find_value(self, key):
        value = self.find_value(key)
        if value:
            return value
        closest_nodes = self.iterative_find_node(Node(hash_function(key), "", 0))
        for node in closest_nodes:
            value = self.find_value(key)
            if value:
                return value
        return None

    def rpc_find_node(self, target_node, node_id):
        # Simular la llamada RPC FIND_NODE
        logger.info(f"RPC FIND_NODE called on {target_node.id} for {node_id}")
        # Aquí debería implementarse la lógica para realizar una llamada RPC real.
        # Esta función debe devolver una lista de nodos encontrados.
        return self.find_node(node_id)

    def start_health_check(self, nodes):
        self.health_check = True

        def run_health_check():
            while self.health_check:
                for node in nodes:
                    self.ping(node)
                time.sleep(60)  # Intervalo de tiempo para el health check

        self.health_check_thread = Thread(target=run_health_check)
        self.health_check_thread.start()

    def stop_health_check(self):
        self.health_check = False
        self.health_check_thread.join()

    def ping(self, node):
        logger.info(f"Pinging node {node.id} at {node.ip}:{node.port}")
        # Aquí se implementaría la lógica para verificar si el nodo está activo.


# Inicializar nodo Kademlia
def initialize_node(node_id, ip, port, is_bootstrap):
    return Node(node_id, ip, port, is_bootstrap)


kademlia_network = None


@socketio.on("ping")
def handle_ping(data):
    new_node = Node.from_dict(data)
    kademlia_network.routing_table.update(new_node)
    logger.info(f"Ping received from node {new_node.id}")
    emit("pong", {"status": "pong"})


@socketio.on("join")
def handle_join(data):
    bootstrap_nodes = [
        Node.from_dict(node_data) for node_data in data["bootstrap_nodes"]
    ]
    kademlia_network.join_network(bootstrap_nodes)
    emit("joined", {"status": "joined"})


def main(args):
    global kademlia_network
    node_id = hash_function(f"{args.address}:{args.port}")
    node = initialize_node(node_id, args.address, args.port, args.master is None)
    kademlia_network = Kademlia(node)

    if args.master:
        bootstrap_ip, bootstrap_port = args.master.split(":")
        bootstrap_node = initialize_node(
            hash_function(f"{bootstrap_ip}:{bootstrap_port}"),
            bootstrap_ip,
            int(bootstrap_port),
            True,
        )
        kademlia_network.join_network([bootstrap_node])

    kademlia_network.start_health_check([node])

    try:
        socketio.run(app, host=args.address, port=args.port)
    finally:
        kademlia_network.stop_health_check()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client of a distributed scraper")
    parser.add_argument(
        "-a", "--address", type=str, default="127.0.0.1", help="node address"
    )
    parser.add_argument("-p", "--port", type=int, default=4142, help="connection port")
    parser.add_argument("-l", "--level", type=str, default="INFO", help="log level")
    parser.add_argument(
        "-d", "--depth", type=int, default=1, help="depth of recursive downloads"
    )
    parser.add_argument(
        "-u",
        "--urls",
        type=str,
        default="urls",
        help="path of file that contains the urls set",
    )
    parser.add_argument(
        "-m",
        "--master",
        type=str,
        default=None,
        help="address of an existing master node. Insert as ip_address:port_number",
    )

    args = parser.parse_args()

    # Configurar el nivel de logging
    logging.getLogger().setLevel(args.level.upper())

    main(args)
