import argparse
import hashlib
import logging
import socket
import sys
import threading
import json
from queue import Queue, Empty
import time


def generate_digest_sha256(string):
    if not isinstance(string, bytes):
        string = str(string).encode("utf8")
    return hashlib.sha256(string).hexdigest()


log = logging.getLogger(__name__)


class LoadBalancer:
    def __init__(self, host="localhost", port=5000, leader=False, bootstrap_nodes=None):
        self.host = host
        self.port = port
        self.node_id = generate_digest_sha256(f"{host}:{port}")
        self.nodes = {}  # Registro de nodos scraper con su estado
        self.kademlia_nodes = {}  # Registro de nodos Kademlia
        self.task_queue = Queue()
        self.lock = threading.Lock()
        self.node_list = []  # Lista de identificadores de nodos registrados
        self.is_leader = leader
        self.leader_info = (self.host, self.port) if leader else None
        self.index_counter = 0
        self.current_index_counter = 0
        self.bootstrap_nodes = bootstrap_nodes or []

    def start(self):
        threading.Thread(target=self.run_server).start()
        threading.Thread(target=self.heartbeat_loop).start()
        if self.bootstrap_nodes:
            threading.Thread(target=self.bootstrap).start()

    def run_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.bind((self.host, self.port))
            server.listen(5)
            log.info(
                f"LoadBalancer Server started at {self.host}:{self.port} --> id: {self.node_id}, Leader: {self.is_leader}"
            )
            while True:
                client, addr = server.accept()
                threading.Thread(target=self.handle_client, args=(client, addr)).start()

    def handle_client(self, conn, addr):
        with conn:
            data = conn.recv(1024)
            if not data:
                return
            message = json.loads(data.decode("utf-8"))
            action = message.get("action")
            if action:
                getattr(self, f"action_{action}", self.unknown_action)(
                    message, conn, addr
                )

    def action_register_node(self, message, conn, addr):
        self.register_node(message["node_id"], addr)
        conn.sendall(
            json.dumps({"index": self.nodes[message["node_id"]]["index"]}).encode(
                "utf-8"
            )
        )

    def action_update_leader(self, message, conn, addr):
        if self.is_leader:
            self.notify_all("update_leader", {"new_leader": self.node_id})

    def action_heartbeat(self, message, conn, addr):
        self.nodes[message["node_id"]]["last_heartbeat"] = time.time()
        log.info(f"Heartbeat received from {message['node_id']}")

    def action_election(self, message, conn, addr):
        if self.is_leader:
            self.start_election()

    def unknown_action(self, message, conn, addr):
        log.warning("Received unknown action")

    def register_node(self, node_id, addr):
        log.info(f"adrr {addr}")
        with self.lock:
            if node_id not in self.nodes:
                self.current_index_counter += 1
                self.nodes[node_id] = {
                    "address": addr[0],
                    "index": self.current_index_counter,
                    "last_heartbeat": time.time(),
                }
                self.node_list.append(node_id)
                log.info(
                    f"Node {node_id} registered with index {self.current_index_counter}"
                )
                self.notify_all(
                    "new_node",
                    {"node_id": node_id, "index": self.current_index_counter},
                )

    def action_new_leader(self, message, conn, addr):
        self.nodes[message["node_id"]] = {
            "address": addr[0],
            "index": message["index"],
            "last_heartbeat": time.time(),
        }
        self.node_list.append(message["node_id"])
        log.info(f"Node {message['node_id']} registered with index {message['index']}")
        self.leader_info = (addr[0], addr[1])

    def notify_all(self, action, message):
        for node_id in self.node_list:
            if node_id != self.node_id:
                self.send_message(self.nodes[node_id]["address"], action, message)

    def send_message(self, address, action, message):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((address, self.port))
            s.send(json.dumps({"action": action, **message}).encode("utf-8"))
            s.close()
        except Exception as e:
            log.error(f"Failed to send message to {address}: {str(e)}")

    def bootstrap(self):
        for address, port in self.bootstrap_nodes:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((address, port))
                s.send(
                    json.dumps(
                        {"action": "register_node", "node_id": self.node_id}
                    ).encode("utf-8")
                )
                data = s.recv(1024)
                response = json.loads(data.decode("utf-8"))
                log.info(
                    f"Bootstrap successful with node at {address}:{port}, assigned index {response['index']}"
                )
                s.close()
            except Exception as e:
                log.error(
                    f"Failed to bootstrap with node at {address}:{port}: {str(e)}"
                )

    def heartbeat_loop(self):
        while True:
            time.sleep(10)
            if not self.is_leader:
                pass
            for node_id, node_info in self.nodes.items():
                if time.time() - node_info["last_heartbeat"] > 15:
                    log.info(f"Node {node_id} is down, initiating election")
                    if self.is_leader:
                        self.start_election()

    def start_election(self):
        highest_index = max((node["index"] for node in self.nodes.values()), default=-1)
        new_leader_id = next(
            node_id
            for node_id, node in self.nodes.items()
            if node["index"] == highest_index
        )
        self.is_leader = self.node_id == new_leader_id
        if self.is_leader:
            log.info(f"New leader elected: {self.node_id}")
            self.notify_all("new_leader", {"new_leader": self.node_id})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Balancer Node")
    parser.add_argument(
        "-a", "--address", type=str, default="127.0.0.1", help="Dirección IP del nodo"
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=5000,
        help="Puerto en el que el nodo escuchará",
    )
    parser.add_argument(
        "-l",
        "--leader",
        action="store_true",
        help="Establecer este nodo como líder inicial",
    )
    parser.add_argument(
        "-ll",
        "--log-level",
        type=str,
        default="INFO",
        help="Nivel de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    parser.add_argument(
        "-b",
        "--bootstrap",
        nargs="+",
        help="Dirección de nodos de bootstrap en formato ip:port",
        default=[],
    )
    args = parser.parse_args()

    bootstrap_nodes = [
        (node.split(":")[0], int(node.split(":")[1])) for node in args.bootstrap
    ]
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), None))

    lb = LoadBalancer(args.address, args.port, args.leader, bootstrap_nodes)
    lb.start()

# import argparse
# import hashlib
# import logging
# import socket
# import sys
# import threading
# import json
# from queue import Queue, Empty
# import time


# def generate_digest_sha256(string):
#     if not isinstance(string, bytes):
#         string = str(string).encode("utf8")
#     return hashlib.sha256(string).hexdigest()


# log = logging.getLogger(__name__)


# class LoadBalancer:
#     def __init__(self, host="localhost", port=5000, leader=False, bootstrap_nodes=None):
#         self.host = host
#         self.port = port
#         self.node_id = generate_digest_sha256(f"{host}:{port}")
#         self.nodes = {}  # Registro de nodos scrapper con su estado
#         self.kademlia_nodes = {}  # Registro de nodos Kademlia
#         self.task_queue = Queue()
#         self.lock = threading.Lock()
#         self.node_list = []  # Lista de identificadores de nodos registrados
#         self.is_leader = leader
#         self.index_counter = 0
#         self.bootstrap_nodes = bootstrap_nodes or []

#     def start(self):
#         threading.Thread(target=self.run_server).start()
#         threading.Thread(target=self.heartbeat_loop).start()
#         # if self.is_leader:
#         #     threading.Thread(target=self.heartbeat_check).start()
#         if self.bootstrap_nodes:
#             threading.Thread(target=self.bootstrap).start()

#     def run_server(self):
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
#             server.bind((self.host, self.port))
#             server.listen(5)
#             log.info(
#                 f"LoadBalancer Server started at {self.host}:{self.port} --> id: {self.node_id}, Leader: {self.is_leader}"
#             )
#             while True:
#                 client, addr = server.accept()
#                 threading.Thread(target=self.handle_client, args=(client, addr)).start()

#     def handle_client(self, conn, addr):
#         with conn:
#             data = conn.recv(1024)
#             if not data:
#                 return
#             message = json.loads(data.decode("utf-8"))

#             action = message.get("action")
#             if action == "register_node":
#                 self.register_node(message["node_id"], addr)
#                 conn.sendall(
#                     json.dumps(
#                         {"index": self.nodes[message["node_id"]]["index"]}
#                     ).encode("utf-8")
#                 )
#             elif action == "update_leader":
#                 if self.is_leader:
#                     self.notify_all("update_leader", {"new_leader": self.node_id})

#             if message["type"] == "heartbeat":
#                 self.nodes[message["node_id"]]["last_heartbeat"] = time.time()
#                 log.info(f"Heartbeat received from {message['node_id']}")

#             if self.is_leader and message["type"] == "election":
#                 self.start_election()

#             # Heartbeat messages
#             if message["type"] == "heartbeat":
#                 self.nodes[message["node_id"]]["last_heartbeat"] = time.time()
#                 log.info(f"Heartbeat received from {message['node_id']}")

#             # Election process
#             if message["type"] == "election":
#                 self.start_election()

#             # Register scrapper nodes
#             if action == "register_scrapper":
#                 self.register_node(message["node_id"], addr)

#             # Register Kademlia nodes
#             if action == "register_kademlia":
#                 self.kademlia_nodes[message["node_id"]] = addr
#                 log.info(f"Kademlia node {message['node_id']} registered")

#             # Update node load
#             if action == "update_load":
#                 if message["node_id"] in self.nodes:
#                     self.nodes[message["node_id"]]["load"] = message["load"]

#             # Request task handling
#             if action == "request_task":
#                 task = self.assign_task(message["node_id"])
#                 conn.sendall(json.dumps({"task": task}).encode("utf-8"))
#                 log.info(f"Task assigned to {message['node_id']}")

#             # Submit data to Kademlia network
#             if action == "submit_data":
#                 self.distribute_to_kademlia(message["data"])
#                 log.info(f"Data submitted to Kademlia")

#     def register_node(self, node_id, addr):
#         with self.lock:
#             if node_id not in self.nodes:
#                 self.nodes[node_id] = {
#                     "address": addr[0],
#                     "index": self.index_counter,
#                     "last_heartbeat": time.time(),
#                 }
#                 self.node_list.append(node_id)
#                 self.index_counter += 1
#                 log.info(
#                     f"Node {node_id} registered with index {self.index_counter - 1}"
#                 )
#                 self.notify_all(
#                     "new_node", {"node_id": node_id, "index": self.index_counter - 1}
#                 )

#     def notify_all(self, action, message):
#         for node_id in self.node_list:
#             if node_id != self.node_id:
#                 self.send_message(self.nodes[node_id]["address"], action, message)

#     def send_message(self, address, action, message):
#         try:
#             s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             s.connect((address, self.port))
#             s.send(json.dumps({"action": action, **message}).encode("utf-8"))
#             s.close()
#         except Exception as e:
#             log.error(f"Failed to send message to {address}: {str(e)}")

#     def bootstrap(self):
#         for address, port in self.bootstrap_nodes:
#             try:
#                 s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#                 s.connect((address, port))
#                 s.send(
#                     json.dumps(
#                         {"action": "register_node", "node_id": self.node_id}
#                     ).encode("utf-8")
#                 )
#                 data = s.recv(1024)
#                 response = json.loads(data.decode("utf-8"))
#                 log.info(
#                     f"Bootstrap successful with node at {address}:{port}, assigned index {response['index']}"
#                 )
#                 s.close()
#             except Exception as e:
#                 log.error(
#                     f"Failed to bootstrap with node at {address}:{port}: {str(e)}"
#                 )

#     def heartbeat_check(self):
#         while True:
#             time.sleep(5)
#             current_time = time.time()
#             for node_id, node_info in list(self.nodes.items()):
#                 if current_time - node_info["last_heartbeat"] > 15:
#                     log.info(f"Node {node_id} is down, removing from list")
#                     self.nodes.pop(node_id)
#                     self.node_list.remove(node_id)
#                     if node_id == self.node_id:
#                         self.start_election()

#     def heartbeat_loop(self):
#         while True:
#             time.sleep(10)  # Adjust timing based on your needs
#             for node_id, node_info in self.nodes.items():
#                 if time.time() - node_info["last_heartbeat"] > 15:
#                     log.info(f"Node {node_id} is down, initiating election")
#                     if self.is_leader:
#                         self.start_election()

#     def start_election(self):
#         # Election logic to determine new leader based on node list or other criteria
#         self.is_leader = True  # Simplified assumption
#         log.info(f"New leader elected: {self.node_id}")

#     def start_election(self):
#         # Implement election logic here
#         pass


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Load Balancer Node")
#     parser.add_argument(
#         "-a", "--address", type=str, default="127.0.0.1", help="Dirección IP del nodo"
#     )
#     parser.add_argument(
#         "-p",
#         "--port",
#         type=int,
#         default=5000,
#         help="Puerto en el que el nodo escuchará",
#     )
#     parser.add_argument(
#         "-l",
#         "--leader",
#         action="store_true",
#         help="Establecer este nodo como líder inicial",
#     )
#     parser.add_argument(
#         "-ll",
#         "--log-level",
#         type=str,
#         default="INFO",
#         help="Nivel de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
#     )
#     parser.add_argument(
#         "-b",
#         "--bootstrap",
#         nargs="+",
#         help="Dirección de nodos de bootstrap en formato ip:port",
#         default=[],
#     )
#     args = parser.parse_args()

#     bootstrap_nodes = [
#         (node.split(":")[0], int(node.split(":")[1])) for node in args.bootstrap
#     ]
#     logging.basicConfig(level=getattr(logging, args.log_level.upper(), None))

#     lb = LoadBalancer(args.address, args.port, args.leader, bootstrap_nodes)
#     lb.start()


# import argparse
# import hashlib
# import logging
# import socket
# import sys
# import threading
# import json
# from queue import Queue, Empty
# import time


# def generate_digest_sha256(string):
#     if not isinstance(string, bytes):
#         string = str(string).encode("utf8")
#     return hashlib.sha256(string).hexdigest()


# log = logging.getLogger(__name__)


# class LoadBalancer:
#     def __init__(self, host="localhost", port=5000, leader=False, bootstrap_nodes=None):
#         self.host = host
#         self.port = port
#         self.node_id = generate_digest_sha256(f"{host}:{port}")
#         self.nodes = {}  # Registro de nodos scrapper con su estado
#         self.kademlia_nodes = {}  # Registro de nodos Kademlia
#         self.task_queue = Queue()
#         self.lock = threading.Lock()
#         self.node_list = []  # Lista de identificadores de nodos registrados
#         self.is_leader = leader
#         self.index_counter = 0
#         self.bootstrap_nodes = bootstrap_nodes or []

#     def start(self):
#         threading.Thread(target=self.run_server).start()
#         if self.is_leader:
#             threading.Thread(target=self.heartbeat_check).start()

#     def run_server(self):
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
#             server.bind((self.host, self.port))
#             server.listen(5)
#             log.info(
#                 f"LoadBalancer Server started at {self.host}:{self.port} --> id: {self.node_id}, Leader: {self.is_leader}"
#             )
#             while True:
#                 client, addr = server.accept()
#                 threading.Thread(target=self.handle_client, args=(client, addr)).start()

#     def handle_client(self, conn, addr):
#         with conn:
#             data = conn.recv(1024)
#             if not data:
#                 return
#             message = json.loads(data.decode("utf-8"))

#             action = message.get("action")
#             if action == "register_node":
#                 self.register_node(message["node_id"], addr)
#                 conn.sendall(
#                     json.dumps(
#                         {"index": self.nodes[message["node_id"]]["index"]}
#                     ).encode("utf-8")
#                 )
#             elif action == "update_leader":
#                 if self.is_leader:
#                     self.notify_all("update_leader", {"new_leader": self.node_id})

#     def register_node(self, node_id, addr):
#         if node_id not in self.nodes:
#             self.nodes[node_id] = {
#                 "address": addr[0],
#                 "index": self.index_counter,
#                 "last_heartbeat": time.time(),
#             }
#             self.node_list.append(node_id)
#             self.index_counter += 1
#             log.info(f"Node {node_id} registered with index {self.index_counter - 1}")

#     def notify_all(self, action, message):
#         for node_id in self.node_list:
#             if node_id != self.node_id:
#                 self.send_message(self.nodes[node_id]["address"], action, message)

#     def send_message(self, host, action, message):
#         try:
#             s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#             s.connect((host, self.port))
#             s.send(json.dumps({"action": action, **message}).encode("utf-8"))
#             s.close()
#         except Exception as e:
#             log.error(f"Failed to send message to {host}: {str(e)}")

#     def heartbeat_check(self):
#         while True:
#             time.sleep(5)
#             current_time = time.time()
#             for node_id, node_info in list(self.nodes.items()):
#                 if current_time - node_info["last_heartbeat"] > 15:
#                     log.info(f"Node {node_id} is down, removing from list")
#                     self.nodes.pop(node_id)
#                     self.node_list.remove(node_id)
#                     if node_id == self.node_id:
#                         self.start_election()

#     def start_election(self):
#         # Implement election logic here
#         pass


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Load Balancer Node")
#     parser.add_argument(
#         "-a", "--address", type=str, default="127.0.0.1", help="Dirección IP del nodo"
#     )
#     parser.add_argument(
#         "-p",
#         "--port",
#         type=int,
#         default=5000,
#         help="Puerto en el que el nodo escuchará",
#     )
#     parser.add_argument(
#         "-l",
#         "--leader",
#         action="store_true",
#         help="Establecer este nodo como líder inicial",
#     )
#     parser.add_argument(
#         "-ll",
#         "--log-level",
#         type=str,
#         default="INFO",
#         help="Nivel de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
#     )
#     args = parser.parse_args()

#     # Configuración del logging
#     numeric_level = getattr(logging, args.log_level.upper(), None)
#     if not isinstance(numeric_level, int):
#         raise ValueError("Invalid log level: %s" % args.log_level)
#     logging.basicConfig(level=numeric_level)

#     # Iniciar el Load Balancer
#     lb = LoadBalancer(args.address, args.port, args.leader)
#     lb.start()
# # if __name__ == "__main__":
# #     port = int(sys.argv[1])
# #     leader = sys.argv[2] == "leader" if len(sys.argv) > 2 else False
# #     lb = LoadBalancer("localhost", port, leader)
# #     lb.start()

# # import hashlib
# # import logging
# # import socket
# # import sys
# # import threading
# # import json
# # from queue import Queue, Empty
# # import time


# # def generate_digest_sha256(string):
# #     if not isinstance(string, bytes):
# #         string = str(string).encode("utf8")
# #     return hashlib.sha256(string).hexdigest()


# # log = logging.getLogger(__name__)
# # logging.basicConfig(level=logging.INFO)


# # class LoadBalancer:
# #     def __init__(self, host="localhost", port=5000, peers=None):
# #         self.host = host
# #         self.port = port
# #         self.node_id = generate_digest_sha256(f"{host}:{port}")
# #         self.nodes = {}  # Registro de nodos scrapper con su estado
# #         self.kademlia_nodes = {}  # Registro de nodos Kademlia
# #         self.task_queue = Queue()
# #         self.lock = threading.Lock()
# #         self.peers = peers if peers else []  # Lista de otros balanceadores
# #         self.node_list = []  # Lista de identificadores de nodos registrados
# #         self.is_leader = False
# #         self.last_heartbeat = time.time()

# #     def start(self):
# #         threading.Thread(target=self.run_server).start()
# #         threading.Thread(target=self.heartbeat_check).start()

# #     def run_server(self):
# #         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
# #             server.bind((self.host, self.port))
# #             server.listen(5)
# #             log.info(
# #                 f"LoadBalancer Server started at {self.host}:{self.port} --> id: {self.node_id}"
# #             )
# #             while True:
# #                 client, addr = server.accept()
# #                 threading.Thread(target=self.handle_client, args=(client, addr)).start()

# #     def handle_client(self, conn, addr):
# #         with conn, self.lock:
# #             data = conn.recv(1024)
# #             if not data:
# #                 return
# #             message = json.loads(data.decode("utf-8"))

# #             if message["type"] == "heartbeat":
# #                 self.last_heartbeat = time.time()
# #                 self.nodes[message["node_id"]]["last_heartbeat"] = time.time()
# #                 log.info(f"Heartbeat received from {message['node_id']}")
# #             if message["type"] == "election":
# #                 self.start_election()
# #             action = message.get("action")
# #             if action == "register_scrapper":
# #                 self.register_node(message["node_id"], addr)
# #             elif action == "register_kademlia":
# #                 self.kademlia_nodes[message["node_id"]] = addr
# #                 log.info(f"Kademlia node {message['node_id']} registered")
# #             elif action == "update_load":
# #                 if message["node_id"] in self.nodes:
# #                     self.nodes[message["node_id"]]["load"] = message["load"]
# #             elif action == "request_task":
# #                 task = self.assign_task(message["node_id"])
# #                 conn.sendall(json.dumps({"task": task}).encode("utf-8"))
# #                 log.info(f"Task assigned to {message['node_id']}")
# #             elif action == "submit_data":
# #                 self.distribute_to_kademlia(message["data"])
# #                 log.info(f"Data submitted to Kademlia")

# #     def register_node(self, node_id, addr):
# #         self.node_list.append(node_id)  # Maintain a list of all registered nodes
# #         self.nodes[node_id] = {
# #             "address": addr[0],
# #             "load": 0,
# #             "last_heartbeat": time.time(),
# #         }
# #         if len(self.node_list) == 1:  # Automatically make the first node the leader
# #             self.is_leader = True
# #             self.notify_leader_change()

# #     def notify_leader_change(self):
# #         for node_id in self.node_list:
# #             s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# #             s.connect((self.nodes[node_id]["address"], self.port))
# #             s.send(
# #                 json.dumps({"type": "update_leader", "leader_id": self.node_id}).encode(
# #                     "utf-8"
# #                 )
# #             )
# #             s.close()

# #     def start_election(self):
# #         self.is_leader = True  # Assume leadership
# #         self.notify_leader_change()  # Notify all nodes about the leadership change

# #     def heartbeat_check(self):
# #         while True:
# #             time.sleep(4)
# #             print(
# #                 f"peers: {self.peers} on port {self.port} and is_leader: {self.is_leader}",
# #             )
# #             # Election trigger if no heartbeat received
# #             if time.time() - self.last_heartbeat > 5 and not self.is_leader:
# #                 log.info("Heartbeat timeout, starting election")
# #                 self.start_election()


# # if __name__ == "__main__":
# #     port = int(sys.argv[1])
# #     peers = [
# #         {"host": peer.split(":")[0], "port": int(peer.split(":")[1])}
# #         for peer in sys.argv[2:]
# #     ]
# #     lb = LoadBalancer("localhost", port, peers)
# #     lb.start()
