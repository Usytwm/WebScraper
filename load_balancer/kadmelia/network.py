from flask import Flask, request, jsonify
from threading import Thread
import requests
from loguru import logger
from kadmelia.node import Node


class NetworkManager:
    def __init__(self, node):
        self.node: Node = node
        self.app = Flask(__name__)
        self.setup_routes()

    def setup_routes(self):
        @self.app.route("/store", methods=["POST"])
        def store():
            data = request.json
            key = data["key"]
            value = data["value"]
            ttl = data.get("ttl", None)
            logger.info(f"Received store request: key={key}, value={value}, ttl={ttl}")
            self.node.handle_store(key, value, ttl)
            return jsonify({"status": "success"}), 200

        @self.app.route("/find_value", methods=["POST"])
        def find_value():
            data = request.json
            key = data["key"]
            logger.info(f"Received find_value request: key={key}")
            value = self.node.handle_find_value(key)
            return jsonify({"key": key, "value": value}), 200

        @self.app.route("/add_peer", methods=["POST"])
        def add_peer():
            data = request.json
            node_id = data["node_id"]
            ip = data["ip"]
            port = data["port"]
            logger.info(f"Received add_peer request: ip={ip}, port={port}")
            self.node.add_peer(node_id, ip, port)
            return jsonify({"status": "peer added"}), 200

        @self.app.route("/get_peers", methods=["GET"])
        def get_peers():
            peers = self.node.get_peers()
            logger.info("Received get_peers request")
            return jsonify({"peers": peers}), 200

    def run(self):
        logger.info(f"Starting node at {self.node.ip}:{self.node.port}")
        Thread(
            target=self.app.run, kwargs={"host": self.node.ip, "port": self.node.port}
        ).start()

    async def bootstrap(self, known_nodes):
        if not known_nodes:
            logger.info(
                "No known nodes provided, starting as the first node in the network."
            )
            return

        for ip, port in known_nodes:
            try:
                # Notificar al nodo bootstrap sobre el nuevo nodo
                logger.info(f"Notifying bootstrap node {ip}:{port} about the new node")
                self._notify_bootstrap_node(ip, port)

                # Intentar conectar con el nodo bootstrap para obtener la lista de nodos
                logger.info(f"Trying to connect to bootstrap node {ip}:{port}")
                url = f"http://{ip}:{port}/get_peers"
                response = requests.get(url)
                if response.status_code == 200:
                    peers = response.json().get("peers", [])
                    for peer_id, peer_ip, peer_port in peers:
                        self.node.add_peer(peer_id, peer_ip, peer_port)
            except requests.exceptions.RequestException as e:
                logger.error(f"Error connecting to {ip}:{port} - {e}")

    def _notify_bootstrap_node(self, bootstrap_ip, bootstrap_port):
        url = f"http://{bootstrap_ip}:{bootstrap_port}/add_peer"
        data = {"node_id": self.node.id, "ip": self.node.ip, "port": self.node.port}
        try:
            response = requests.post(url, json=data)
            if response.status_code == 200:
                logger.info(
                    f"Successfully notified bootstrap node {bootstrap_ip}:{bootstrap_port} about new node"
                )
            else:
                logger.error(
                    f"Failed to notify bootstrap node {bootstrap_ip}:{bootstrap_port}"
                )
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Error notifying bootstrap node {bootstrap_ip}:{bootstrap_port} - {e}"
            )
